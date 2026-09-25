//! Slipstream: txs submitted straight to this mempool, instead of mirrored
//! from the node's.
//!
//! Every other tx here is a copy of one in the node's mempool, so the node
//! settles it: it refuses double spends, evicts what a block conflicts with,
//! and drops the descendants of anything it removes, and the sync task follows
//! along. A slipstream tx is never in the node's mempool -- that is the point
//! of it, it is never relayed -- so none of that ever reaches it. This pool is
//! the index the sync task needs to do that work itself.
//!
//! Invariant: every tx in [`SlipstreamPool`] is also in the [`Mempool`].

use std::collections::{HashMap, HashSet};

use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid, Wtxid};
use hashlink::LinkedHashMap;
use lender::FallibleLender as _;
use serde::{Deserialize, Serialize};

use crate::mempool::{Mempool, MissingAncestorError};

/// How many removed txs are remembered, so that a submitter asking after one
/// learns why it left rather than that it is unknown.
const REMOVED_HISTORY: usize = 10_000;

#[derive(Clone, Debug)]
pub struct SlipstreamTx {
    pub tx: Transaction,
    pub fee: Amount,
    pub sigop_cost: usize,
    /// Unix time, in seconds
    pub submitted_at: u64,
}

/// Why a slipstream tx left the mempool
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum SlipstreamRemoval {
    /// Confirmed by a block
    Mined { block_hash: BlockHash },
    /// A block spent one of its inputs in another tx
    ConflictMined {
        block_hash: BlockHash,
        spent_by: Txid,
    },
    /// A parent it spends left the node's mempool without being mined
    ParentRemoved { parent: Txid },
    /// Removed by the enforcer's rules when a block connected or disconnected
    RejectedByEnforcer,
    /// A block was disconnected. A reorg can leave a tx invalid -- a parent
    /// that does not return, a coinbase no longer mature, a timelock no
    /// longer met -- and the node re-checks its own txs, but never had this
    /// one. It has to be submitted again.
    Reorged { block_hash: BlockHash },
    /// Removed on request
    Withdrawn,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SlipstreamTxInfo {
    pub txid: Txid,
    pub wtxid: Wtxid,
    pub fee_sat: u64,
    pub vsize: u64,
    pub weight: u64,
    pub sigop_cost: u64,
    pub submitted_at: u64,
}

impl From<&SlipstreamTx> for SlipstreamTxInfo {
    fn from(entry: &SlipstreamTx) -> Self {
        Self {
            txid: entry.tx.compute_txid(),
            wtxid: entry.tx.compute_wtxid(),
            fee_sat: entry.fee.to_sat(),
            vsize: entry.tx.vsize() as u64,
            weight: entry.tx.weight().to_wu(),
            sigop_cost: entry.sigop_cost as u64,
            submitted_at: entry.submitted_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum SlipstreamTxStatus {
    /// In the mempool, competing for inclusion by fee rate
    Pending(SlipstreamTxInfo),
    Removed {
        txid: Txid,
        #[serde(flatten)]
        removal: SlipstreamRemoval,
    },
    /// Never submitted, or removed long enough ago to be forgotten
    Unknown { txid: Txid },
}

#[derive(Debug, Default)]
pub struct SlipstreamPool {
    txs: HashMap<Txid, SlipstreamTx>,
    /// The slipstream tx spending each outpoint
    spends: HashMap<OutPoint, Txid>,
    removed: LinkedHashMap<Txid, SlipstreamRemoval>,
    /// Sum of `sigop_cost` over `txs`
    total_sigop_cost: usize,
}

impl SlipstreamPool {
    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn contains(&self, txid: &Txid) -> bool {
        self.txs.contains_key(txid)
    }

    pub fn iter(&self) -> impl Iterator<Item = &SlipstreamTx> {
        self.txs.values()
    }

    pub fn txids(&self) -> impl Iterator<Item = &Txid> {
        self.txs.keys()
    }

    /// Sigop cost of every slipstream tx together
    pub fn total_sigop_cost(&self) -> usize {
        self.total_sigop_cost
    }

    pub fn status(&self, txid: Txid) -> SlipstreamTxStatus {
        if let Some(entry) = self.txs.get(&txid) {
            SlipstreamTxStatus::Pending(entry.into())
        } else if let Some(removal) = self.removed.get(&txid) {
            SlipstreamTxStatus::Removed {
                txid,
                removal: removal.clone(),
            }
        } else {
            SlipstreamTxStatus::Unknown { txid }
        }
    }

    pub(in crate::mempool) fn insert(&mut self, entry: SlipstreamTx) {
        let txid = entry.tx.compute_txid();
        for input in &entry.tx.input {
            self.spends.insert(input.previous_output, txid);
        }
        self.removed.remove(&txid);
        self.total_sigop_cost += entry.sigop_cost;
        if let Some(replaced) = self.txs.insert(txid, entry) {
            self.total_sigop_cost -= replaced.sigop_cost;
        }
    }

    /// Returns the tx if it was in the pool
    pub(in crate::mempool) fn remove(
        &mut self,
        txid: &Txid,
        reason: SlipstreamRemoval,
    ) -> Option<SlipstreamTx> {
        let entry = self.txs.remove(txid)?;
        self.total_sigop_cost -= entry.sigop_cost;
        for input in &entry.tx.input {
            if self.spends.get(&input.previous_output) == Some(txid) {
                self.spends.remove(&input.previous_output);
            }
        }
        self.removed.replace(*txid, reason);
        while self.removed.len() > REMOVED_HISTORY {
            self.removed.pop_front();
        }
        Some(entry)
    }

    /// The slipstream tx spending `outpoint`, if any
    pub(in crate::mempool) fn spender(
        &self,
        outpoint: &OutPoint,
    ) -> Option<Txid> {
        self.spends.get(outpoint).copied()
    }

    /// Slipstream txs other than `tx` that spend an outpoint `tx` spends
    pub(in crate::mempool) fn conflicts(
        &self,
        tx: &Transaction,
    ) -> HashSet<Txid> {
        let txid = tx.compute_txid();
        tx.input
            .iter()
            .filter_map(|input| self.spender(&input.previous_output))
            .filter(|spender| *spender != txid)
            .collect()
    }

    /// Slipstream txs spending an output of `parent`
    pub(in crate::mempool) fn children_of(
        &self,
        parent: &Txid,
    ) -> HashSet<Txid> {
        self.spends
            .iter()
            .filter(|(outpoint, _)| outpoint.txid == *parent)
            .map(|(_, child)| *child)
            .collect()
    }

    /// Slipstream txs that are no longer in `mempool`
    pub(in crate::mempool) fn missing_from(
        &self,
        mempool: &Mempool,
    ) -> Vec<Txid> {
        self.txs
            .keys()
            .filter(|txid| !mempool.txs.0.contains_key(*txid))
            .copied()
            .collect()
    }
}

impl Mempool {
    /// The mempool txs that spend `outpoint`
    pub(in crate::mempool) fn spenders_of(
        &self,
        outpoint: &OutPoint,
    ) -> Vec<Txid> {
        let Some(childs) = self.tx_childs.0.get(&outpoint.txid) else {
            return Vec::new();
        };
        childs
            .iter()
            .filter(|child| {
                self.txs.0.get(*child).is_some_and(|(child_tx, _)| {
                    child_tx
                        .input
                        .iter()
                        .any(|input| input.previous_output == *outpoint)
                })
            })
            .copied()
            .collect()
    }

    /// `parents` and all of their in-mempool ancestors, each with its fee,
    /// ordered so that every tx follows the txs it spends. This is the
    /// package a tx spending `parents` has to be mined with.
    pub fn package_of(
        &self,
        parents: &[Txid],
    ) -> Result<Vec<(Transaction, Amount)>, MissingAncestorError> {
        let mut visited = HashSet::new();
        let mut res = Vec::new();
        for parent in parents {
            if visited.contains(parent) {
                continue;
            }
            let mut ancestors = self.txs.ancestors(*parent);
            while let Some((anc_txid, anc_tx, anc_info)) = ancestors.next()? {
                if visited.insert(anc_txid) {
                    res.push((anc_tx.clone(), anc_info.fees.base));
                }
            }
            let (tx, info) =
                self.txs.0.get(parent).ok_or(MissingAncestorError {
                    tx: *parent,
                    missing: *parent,
                })?;
            visited.insert(*parent);
            res.push((tx.clone(), info.fees.base));
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute::LockTime,
        hashes::Hash as _, transaction::Version,
    };

    use super::*;

    fn make_tx(inputs: &[OutPoint], value: u64) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: inputs
                .iter()
                .map(|prev| TxIn {
                    previous_output: *prev,
                    sequence: Sequence::MAX,
                    script_sig: ScriptBuf::new(),
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(value),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn entry(tx: Transaction) -> SlipstreamTx {
        SlipstreamTx {
            tx,
            fee: Amount::from_sat(1_000),
            sigop_cost: 4,
            submitted_at: 0,
        }
    }

    #[test]
    fn removal_is_remembered_and_cleared_on_resubmit() {
        let mut pool = SlipstreamPool::default();
        let tx = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1_000);
        let txid = tx.compute_txid();
        pool.insert(entry(tx.clone()));
        assert!(matches!(pool.status(txid), SlipstreamTxStatus::Pending(_)));

        let removed = pool.remove(&txid, SlipstreamRemoval::Withdrawn);
        assert!(removed.is_some());
        assert_eq!(
            pool.status(txid),
            SlipstreamTxStatus::Removed {
                txid,
                removal: SlipstreamRemoval::Withdrawn
            }
        );
        assert_eq!(pool.spender(&tx.input[0].previous_output), None);

        pool.insert(entry(tx));
        assert!(matches!(pool.status(txid), SlipstreamTxStatus::Pending(_)));
    }

    #[test]
    fn total_sigop_cost_follows_inserts_and_removals() {
        let mut pool = SlipstreamPool::default();
        let a = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1_000);
        let b = make_tx(&[OutPoint::new(Txid::all_zeros(), 1)], 1_000);
        pool.insert(entry(a.clone()));
        pool.insert(entry(b));
        // Re-inserting the same tx must not count it twice
        pool.insert(entry(a.clone()));
        assert_eq!(pool.total_sigop_cost(), 8);
        let _removed =
            pool.remove(&a.compute_txid(), SlipstreamRemoval::Withdrawn);
        assert_eq!(pool.total_sigop_cost(), 4);
    }

    #[test]
    fn conflicts_are_shared_outpoints_only() {
        let mut pool = SlipstreamPool::default();
        let shared = OutPoint::new(Txid::all_zeros(), 0);
        let other = OutPoint::new(Txid::all_zeros(), 1);
        let slipstream = make_tx(&[shared], 1_000);
        let slipstream_txid = slipstream.compute_txid();
        pool.insert(entry(slipstream.clone()));

        // A double spend of `shared` conflicts; a spend of `other` does not,
        // and a tx never conflicts with itself.
        assert_eq!(
            pool.conflicts(&make_tx(&[shared], 2_000)),
            HashSet::from([slipstream_txid])
        );
        assert!(pool.conflicts(&make_tx(&[other], 2_000)).is_empty());
        assert!(pool.conflicts(&slipstream).is_empty());
    }

    #[test]
    fn children_of_spent_parent() {
        let mut pool = SlipstreamPool::default();
        let parent = Txid::from_byte_array([7; 32]);
        let child = make_tx(&[OutPoint::new(parent, 3)], 1_000);
        let unrelated = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1_000);
        pool.insert(entry(child.clone()));
        pool.insert(entry(unrelated));
        assert_eq!(
            pool.children_of(&parent),
            HashSet::from([child.compute_txid()])
        );
    }

    #[test]
    fn status_serializes_flat() {
        let txid = Txid::all_zeros();
        let block_hash = BlockHash::all_zeros();
        let status = SlipstreamTxStatus::Removed {
            txid,
            removal: SlipstreamRemoval::Mined { block_hash },
        };
        let json = serde_json_value(&status);
        assert_eq!(json["status"], "removed");
        assert_eq!(json["reason"], "mined");
        assert_eq!(json["block_hash"], block_hash.to_string());
    }

    fn serde_json_value<T: Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).unwrap()
    }
}
