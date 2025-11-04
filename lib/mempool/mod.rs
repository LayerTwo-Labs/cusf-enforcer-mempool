use std::collections::{BTreeMap, HashMap, VecDeque};

use bitcoin::{BlockHash, Network, Target, Transaction, Txid, Weight};
use bitcoin_jsonrpsee::client::{BlockTemplateTransaction, RawMempoolTxFees};
use hashlink::{LinkedHashMap, LinkedHashSet};
use imbl::{ordmap, OrdMap, OrdSet};
use indexmap::IndexSet;
use lending_iterator::LendingIterator as _;
use thiserror::Error;

pub mod iter;
pub mod iter_mut;
mod sync;

pub use sync::{
    init_sync_mempool, task::SyncTaskError, InitialSyncMempoolError,
    MempoolSync,
};

#[derive(Clone, Copy, Debug, Eq)]
pub struct FeeRate {
    fee: u64,
    vsize: u64,
}

impl Ord for FeeRate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // (self.fee / self.size) > (other.fee / other.size) ==>
        // (self.fee * other.size) > (other.fee * self.size)
        let lhs = self.fee as u128 * other.vsize as u128;
        let rhs = other.fee as u128 * self.vsize as u128;
        lhs.cmp(&rhs)
    }
}

impl PartialEq for FeeRate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd for FeeRate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct TxInfo {
    pub ancestor_modified_weight: Weight,
    pub ancestor_vsize: u64,
    pub bip125_replaceable: bool,
    pub depends: OrdSet<Txid>,
    pub descendant_modified_weight: Weight,
    pub descendant_vsize: u64,
    pub fees: RawMempoolTxFees,
    pub modified_weight: Weight,
    pub spent_by: OrdSet<Txid>,
    /// Conflicts due to reasons other than shared inputs
    pub conflicts_with: OrdSet<Txid>,
}

#[derive(Debug, Error)]
#[error("Missing ancestor for {tx}: {missing}")]
pub struct MissingAncestorError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendant for {tx}: {missing}")]
pub struct MissingDescendantError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendants key: {0}")]
pub struct MissingDescendantsKeyError(Txid);

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolInsertError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
    #[error("Tx already exists in mempool (`{txid}`)")]
    TxAlreadyExists { txid: Txid },
}

#[derive(Debug, Error)]
#[error("Missing by_ancestor_fee_rate key: {0:?}")]
pub struct MissingByAncestorFeeRateKeyError(FeeRate);

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolRemoveError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingByAncestorFeeRateKey(#[from] MissingByAncestorFeeRateKeyError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
}

#[derive(Debug, Error)]
pub enum MempoolUpdateError {
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
}

/// Description of conflicts between txs
#[derive(Debug, Default)]
pub(crate) struct Conflicts(
    /// Conflicts between txids a and b, where a < b, are stored by inserting
    /// b into the set of conflicts with a.
    BTreeMap<Txid, std::collections::HashSet<Txid>>,
);

impl Conflicts {
    fn insert(&mut self, txid_a: Txid, txid_b: Txid) {
        let (txid_lo, txid_hi) = match txid_a.cmp(&txid_b) {
            std::cmp::Ordering::Less => (txid_a, txid_b),
            std::cmp::Ordering::Equal => return,
            std::cmp::Ordering::Greater => (txid_b, txid_a),
        };
        self.0.entry(txid_lo).or_default().insert(txid_hi);
    }

    /// Iterate over conflicts, where the first element is the lower txid.
    /// Each conflict will be visited only once - if txids `a` and `b` conflict,
    /// where `a < b`, the conflict will be expressed as `(a, conflicts_a)`,
    /// where `b` is an element of `conflicts_a`.
    /// If `b` is visited, as `(b, conflicts_b)`, then `a` will not be an
    /// element of `conflicts_b`.
    fn iter(
        &self,
    ) -> impl Iterator<Item = (&Txid, &std::collections::HashSet<Txid>)> {
        self.0.iter()
    }

    /// Iterate over conflicts, where the first element is the greater txid.
    /// Each conflict will be visited only once - if txids `a` and `b` conflict,
    /// where `a > b`, the conflict will be expressed as `(a, conflicts_a)`,
    /// where `b` is an element of `conflicts_a`.
    /// If `b` is visited, as `(b, conflicts_b)`, then `a` will not be an
    /// element of `conflicts_b`.
    fn iter_inverted(
        self,
    ) -> impl Iterator<Item = (Txid, std::collections::HashSet<Txid>)> {
        let mut inverted =
            BTreeMap::<Txid, std::collections::HashSet<Txid>>::new();
        for (txid_lo, conflicts) in self.0 {
            for txid_hi in conflicts {
                inverted.entry(txid_hi).or_default().insert(txid_lo);
            }
        }
        inverted.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
struct ByAncestorFeeRate(OrdMap<FeeRate, LinkedHashSet<Txid>>);

impl ByAncestorFeeRate {
    fn insert(&mut self, fee_rate: FeeRate, txid: Txid) {
        self.0.entry(fee_rate).or_default().insert(txid);
    }

    /// returns `true` if removed successfully, or `false` if not found
    fn remove(&mut self, fee_rate: FeeRate, txid: Txid) -> bool {
        match self.0.entry(fee_rate) {
            ordmap::Entry::Occupied(mut entry) => {
                let txs = entry.get_mut();
                txs.remove(&txid);
                if txs.is_empty() {
                    entry.remove();
                }
                true
            }
            ordmap::Entry::Vacant(_) => false,
        }
    }

    /// Iterate from low-to-high fee rate, in insertion order
    #[allow(dead_code)]
    fn iter(&self) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (*fee_rate, *txid))
        })
    }

    /// Iterate from high-to-low fee rate, in insertion order
    fn iter_rev(
        &self,
    ) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().rev().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (*fee_rate, *txid))
        })
    }
}

#[derive(Clone, Debug)]
struct Chain {
    tip: BlockHash,
    blocks: imbl::HashMap<BlockHash, bitcoin_jsonrpsee::client::Block<true>>,
}

impl Chain {
    // Iterate over blocks from tip towards genesis.
    // Not all history is guaranteed to exist, so this iterator might return
    // `None` before the genesis block.
    #[allow(dead_code)]
    fn iter(
        &self,
    ) -> impl Iterator<Item = &bitcoin_jsonrpsee::client::Block<true>> {
        let mut next = Some(self.tip);
        std::iter::from_fn(move || {
            if let Some(block) = self.blocks.get(&next?) {
                next = block.previousblockhash;
                Some(block)
            } else {
                next = None;
                None
            }
        })
    }
}

/// Map of txs (which may not be in the mempool) to their direct child txs,
/// which MUST be in the mempool
#[derive(Clone, Debug, Default)]
struct TxChilds(imbl::HashMap<Txid, imbl::HashSet<Txid>>);

impl TxChilds {
    fn insert(&mut self, txid: Txid, child: Txid) -> bool {
        self.0.entry(txid).or_default().insert(child).is_some()
    }

    fn remove(&mut self, txid: Txid, child: Txid) -> bool {
        match self.0.entry(txid) {
            imbl::hashmap::Entry::Occupied(mut entry) => {
                let res = entry.get_mut().remove(&child).is_some();
                if entry.get().is_empty() {
                    entry.remove();
                }
                res
            }
            imbl::hashmap::Entry::Vacant(_) => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MempoolTxs(imbl::HashMap<Txid, (Transaction, TxInfo)>);

pub const MAX_USABLE_BLOCK_WEIGHT: Weight = {
    const COINBASE_TXIN_WEIGHT: Weight = {
        let weight_wu = Weight::from_non_witness_data_size(
            // outpoint
            36
            // sequence
            + 4
            // SPK
            + 5
        ).to_wu()
        // witness
        + Weight::from_witness_data_size(33).to_wu();
        Weight::from_wu(weight_wu)
    };
    const COINBASE_VALUE_TXOUT_WEIGHT: Weight = {
        let weight_wu = Weight::from_non_witness_data_size(bitcoin::Amount::SIZE as u64).to_wu()
            // SPK weight
            + Weight::from_non_witness_data_size(23).to_wu();
        Weight::from_wu(weight_wu)
    };
    const COINBASE_TX_WEIGHT: Weight = Weight::from_wu(
        // version
        Weight::from_non_witness_data_size(4).to_wu()
        // locktime
        + Weight::from_non_witness_data_size(4).to_wu()
        // inputs
        + Weight::from_non_witness_data_size(1).to_wu()
        + COINBASE_TXIN_WEIGHT.to_wu()
        // outputs
        + Weight::from_non_witness_data_size(1).to_wu()
        + COINBASE_VALUE_TXOUT_WEIGHT.to_wu(),
    );
    let res_wu = Weight::MAX_BLOCK.to_wu() - Weight::from_non_witness_data_size(
            bitcoin::block::Header::SIZE as u64
        ).to_wu()
            // 2 bytes for encoding txs array length
            - Weight::from_non_witness_data_size(2).to_wu()
            - COINBASE_TX_WEIGHT.to_wu();
    Weight::from_wu(res_wu)
};

#[inline(always)]
fn saturating_add_weight(lhs: Weight, rhs: Weight) -> Weight {
    let res_wu = lhs.to_wu().saturating_add(rhs.to_wu());
    Weight::from_wu(res_wu)
}

#[inline(always)]
fn saturating_add_signed_weight(lhs: Weight, rhs_wu: i64) -> Weight {
    let res_wu = lhs.to_wu().saturating_add_signed(rhs_wu);
    Weight::from_wu(res_wu)
}

#[inline(always)]
fn saturating_sub_weight(lhs: Weight, rhs: Weight) -> Weight {
    let res_wu = lhs.to_wu().saturating_sub(rhs.to_wu());
    Weight::from_wu(res_wu)
}

// MUST be cheap to clone so that constructing block templates is cheap
#[derive(Clone, Debug)]
pub struct Mempool {
    by_ancestor_fee_rate: ByAncestorFeeRate,
    chain: Chain,
    network: Network,
    /// Map of txs (which may not be in the mempool) to their direct child txs,
    /// which MUST be in the mempool
    tx_childs: TxChilds,
    txs: MempoolTxs,
}

impl Mempool {
    fn new(network: Network, prev_blockhash: BlockHash) -> Self {
        let chain = Chain {
            tip: prev_blockhash,
            blocks: imbl::HashMap::new(),
        };
        Self {
            by_ancestor_fee_rate: ByAncestorFeeRate::default(),
            chain,
            network,
            tx_childs: TxChilds::default(),
            txs: MempoolTxs::default(),
        }
    }

    pub fn tip(&self) -> &bitcoin_jsonrpsee::client::Block<true> {
        &self.chain.blocks[&self.chain.tip]
    }

    /// Next target, if known
    pub fn next_target(&self) -> Option<Target> {
        let tip = self.tip();
        let next_height = tip.height + 1;
        let network_params = self.network.params();
        if !network_params.no_pow_retargeting
            && next_height % network_params.miner_confirmation_window == 0
        {
            if let Some(first_block_in_period) = self
                .chain
                .iter()
                .nth(network_params.miner_confirmation_window as usize - 1)
            {
                let spacing = tip.time - first_block_in_period.time;
                let res = bitcoin::CompactTarget::from_next_work_required(
                    tip.compact_target,
                    spacing as u64,
                    network_params,
                );
                Some(res.into())
            } else {
                None
            }
        } else {
            Some(tip.compact_target.into())
        }
    }

    /// Insert a tx into the mempool,
    /// stating conflicts with other txs due to reasons other than shared
    /// inputs.
    pub fn insert(
        &mut self,
        tx: Transaction,
        fee: u64,
        conflicts_with: imbl::OrdSet<Txid>,
        modified_weight: Weight,
    ) -> Result<Option<TxInfo>, MempoolInsertError> {
        let txid = tx.compute_txid();
        if self.txs.0.contains_key(&txid) {
            return Err(MempoolInsertError::TxAlreadyExists { txid });
        }
        // initially incorrect, must be computed after insertion
        let mut ancestor_fees = fee;
        // initially incorrect, must be computed after insertion
        let mut descendant_fees = fee;
        let modified_fee = fee;
        let vsize = tx.vsize() as u64;
        // initially incorrect, must be computed after insertion
        let mut ancestor_modified_weight = modified_weight;
        // initially incorrect, must be computed after insertion
        let mut ancestor_vsize = vsize;
        // initially incorrect, must be computed after insertion
        let mut descendant_modified_weight = modified_weight;
        // initially incorrect, must be computed after insertion
        let mut descendant_vsize = vsize;
        // conflicts including ancestor conflicts
        let mut ancestry_conflicts = conflicts_with.clone();
        let depends = tx
            .input
            .iter()
            .map(|input| {
                let input_txid = input.previous_output.txid;
                self.tx_childs.insert(input_txid, txid);
                input_txid
            })
            .filter(|input_txid| self.txs.0.contains_key(input_txid))
            .collect();
        for dep in &depends {
            let (_, dep_info) =
                self.txs.0.get_mut(dep).ok_or(MissingAncestorError {
                    tx: txid,
                    missing: *dep,
                })?;
            dep_info.spent_by.insert(txid);
            ancestry_conflicts =
                ancestry_conflicts.union(dep_info.conflicts_with.clone());
        }
        let spent_by = if let Some(childs) = self.tx_childs.0.get(&txid) {
            OrdSet::from_iter(childs.iter().copied())
        } else {
            OrdSet::new()
        };
        let info = TxInfo {
            ancestor_modified_weight,
            ancestor_vsize,
            bip125_replaceable: tx.is_explicitly_rbf(),
            depends,
            descendant_modified_weight,
            descendant_vsize,
            fees: RawMempoolTxFees {
                ancestor: ancestor_fees,
                base: fee,
                descendant: descendant_fees,
                modified: modified_fee,
            },
            modified_weight,
            spent_by,
            conflicts_with: ancestry_conflicts,
        };
        let (ndeps, nspenders) = (info.depends.len(), info.spent_by.len());
        let res = self.txs.0.insert(txid, (tx, info)).map(|(_, info)| info);
        tracing::debug!(
            fee = %bitcoin::Amount::from_sat(fee).display_dynamic(),
            modified_fee = %bitcoin::Amount::from_sat(modified_fee).display_dynamic(),
            %txid,
            "Inserted tx into mempool with {ndeps} deps and {nspenders} spenders"
        );
        self.txs.ancestors_mut(txid).try_for_each(|ancestor_info| {
            let (ancestor_tx, ancestor_info) = ancestor_info?;
            ancestor_vsize += ancestor_tx.vsize() as u64;
            ancestor_modified_weight = saturating_add_weight(
                ancestor_modified_weight,
                ancestor_info.modified_weight,
            );
            ancestor_fees += ancestor_info.fees.modified;
            ancestor_info.descendant_vsize += vsize;
            ancestor_info.descendant_modified_weight = saturating_add_weight(
                ancestor_info.descendant_modified_weight,
                modified_weight,
            );
            ancestor_info.fees.descendant += modified_fee;
            Result::<_, MempoolInsertError>::Ok(())
        })?;
        self.txs.descendants_mut(txid).skip(1).try_for_each(
            |descendant_info| {
                let (descendant_tx, descendant_info) = descendant_info?;
                descendant_vsize += descendant_tx.vsize() as u64;
                descendant_modified_weight = saturating_add_weight(
                    descendant_modified_weight,
                    descendant_info.modified_weight,
                );
                descendant_fees += descendant_info.fees.modified;
                descendant_info.ancestor_vsize += vsize;
                descendant_info.ancestor_modified_weight =
                    saturating_add_weight(
                        descendant_info.ancestor_modified_weight,
                        modified_weight,
                    );
                descendant_info.fees.ancestor += modified_fee;
                descendant_info.conflicts_with = descendant_info
                    .conflicts_with
                    .clone()
                    .union(conflicts_with.clone());
                Result::<_, MempoolInsertError>::Ok(())
            },
        )?;
        for conflict_txid in conflicts_with {
            self.txs.descendants_mut(conflict_txid).try_for_each(
                |descendant_info| {
                    let (_descendant_tx, descendant_info) = descendant_info?;
                    descendant_info.conflicts_with.insert(txid);
                    Result::<_, MempoolInsertError>::Ok(())
                },
            )?;
        }
        let (_, info) = self.txs.0.get_mut(&txid).unwrap();
        info.fees.ancestor = ancestor_fees;
        info.fees.descendant = descendant_fees;
        info.ancestor_modified_weight = ancestor_modified_weight;
        info.ancestor_vsize = ancestor_vsize;
        info.descendant_modified_weight = descendant_modified_weight;
        info.descendant_vsize = descendant_vsize;
        let ancestor_fee_rate = FeeRate {
            fee: ancestor_fees,
            vsize: ancestor_modified_weight.to_vbytes_ceil(),
        };
        self.by_ancestor_fee_rate.insert(ancestor_fee_rate, txid);
        Ok(res)
    }

    /// Remove a tx from the mempool. Descendants are updated but not removed.
    fn remove(
        &mut self,
        txid: &Txid,
    ) -> Result<Option<(Transaction, TxInfo)>, MempoolRemoveError> {
        let Some((tx, info)) = self.txs.0.get(txid) else {
            return Ok(None);
        };
        let ancestor_modified_weight = info.ancestor_modified_weight;
        let modified_weight = info.modified_weight;
        let vsize = tx.vsize() as u64;
        let fees = RawMempoolTxFees { ..info.fees };
        for spent_tx in tx.input.iter().map(|input| input.previous_output.txid)
        {
            self.tx_childs.remove(spent_tx, *txid);
        }
        let mut descendants = self.txs.descendants_mut(*txid);
        // Skip first element
        let _: Option<_> = descendants.next().transpose()?;
        let () = descendants.try_for_each(|desc| {
            let (desc_tx, desc_info) = desc?;
            let ancestor_fee_rate = FeeRate {
                fee: desc_info.fees.ancestor,
                vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
            };
            let desc_txid = desc_tx.compute_txid();
            if !self
                .by_ancestor_fee_rate
                .remove(ancestor_fee_rate, desc_txid)
            {
                let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
                return Err(err.into());
            };
            desc_info.ancestor_modified_weight = saturating_sub_weight(
                desc_info.ancestor_modified_weight,
                modified_weight,
            );
            desc_info.ancestor_vsize -= vsize;
            desc_info.fees.ancestor -= fees.modified;
            let ancestor_fee_rate = FeeRate {
                fee: desc_info.fees.ancestor,
                vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
            };
            self.by_ancestor_fee_rate
                .insert(ancestor_fee_rate, desc_txid);
            // FIXME: remove
            tracing::debug!("removing {txid} as a dep of {desc_txid}");
            desc_info.depends.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        // Update all ancestors
        let () = self.txs.ancestors_mut(*txid).try_for_each(|anc| {
            let (anc_tx, anc_info) = anc?;
            anc_info.descendant_modified_weight = saturating_sub_weight(
                anc_info.descendant_modified_weight,
                modified_weight,
            );
            anc_info.descendant_vsize -= vsize;
            anc_info.fees.descendant -= fees.modified;
            let anc_txid = anc_tx.compute_txid();
            // FIXME: remove
            tracing::debug!("removing {txid} as a spender of {anc_txid}");
            anc_info.spent_by.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        let ancestor_fee_rate = FeeRate {
            fee: fees.ancestor,
            vsize: ancestor_modified_weight.to_vbytes_ceil(),
        };
        // Update `self.by_ancestor_fee_rate`
        if !self.by_ancestor_fee_rate.remove(ancestor_fee_rate, *txid) {
            let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
            return Err(err.into());
        };
        let res = self.txs.0.remove(txid);
        // FIXME: remove
        tracing::debug!("Removed {txid} from mempool");
        Ok(res)
    }

    /// Remove a tx from mempool, and all descendants.
    /// Returns the removed tx and descendants.
    fn remove_with_descendants(
        &mut self,
        txid: &Txid,
    ) -> Result<LinkedHashMap<Txid, Transaction>, MempoolRemoveError> {
        let mut res = LinkedHashMap::new();
        let mut remove_stack = VecDeque::from_iter([*txid]);
        while let Some(next) = remove_stack.pop_front() {
            let Some((tx, tx_info)) = self.remove(&next)? else {
                continue;
            };
            remove_stack.extend(tx_info.spent_by);
            res.replace(next, tx);
        }
        Ok(res)
    }

    /// Retain txs for which the provided closure returns `true`.
    /// The closure's second argument is the in-mempool input txs for the
    /// transaction.
    /// If the bool argument is `true, also deletes descendants of any deleted
    /// tx.
    /// Returns the removed txs.
    pub fn try_filter<F, E>(
        &mut self,
        also_remove_descendants: bool,
        mut f: F,
    ) -> Result<
        LinkedHashMap<Txid, Transaction>,
        either::Either<MempoolRemoveError, E>,
    >
    where
        F: FnMut(&Transaction, &HashMap<Txid, &Transaction>) -> Result<bool, E>,
    {
        let no_ancestors_txids: Vec<Txid> = self
            .txs
            .0
            .iter()
            .filter_map(|(txid, (_tx, tx_info))| {
                if tx_info.depends.is_empty() {
                    Some(*txid)
                } else {
                    None
                }
            })
            .collect();
        let mut res = LinkedHashMap::new();
        for txid in no_ancestors_txids {
            let mut descendants = Vec::<Txid>::new();
            let () = self
                .txs
                .descendants_mut(txid)
                .try_for_each(|item| {
                    let (tx, _info) = item?;
                    let descendant_txid = tx.compute_txid();
                    descendants.push(descendant_txid);
                    Result::<_, MempoolRemoveError>::Ok(())
                })
                .map_err(either::Either::Left)?;
            'descs: for descendant_txid in descendants {
                let Some((tx, _info)) = self.txs.0.get(&descendant_txid) else {
                    continue 'descs;
                };
                let mut tx_inputs = HashMap::<Txid, &Transaction>::new();
                'tx_inputs: for tx_in in &tx.input {
                    let input_txid = tx_in.previous_output.txid;
                    if tx_inputs.contains_key(&input_txid) {
                        continue 'tx_inputs;
                    }
                    if let Some((input_tx, _)) = self.txs.0.get(&input_txid) {
                        tx_inputs.insert(input_txid, input_tx);
                    }
                }
                if !f(tx, &tx_inputs).map_err(either::Either::Right)? {
                    let removed = if also_remove_descendants {
                        self.remove_with_descendants(&descendant_txid)
                            .map_err(either::Either::Left)?
                    } else {
                        self.remove(&descendant_txid)
                            .map_err(either::Either::Left)?
                            .into_iter()
                            .map(|(tx, _tx_info)| (descendant_txid, tx))
                            .collect()
                    };
                    res.extend(removed);
                }
            }
        }
        Ok(res)
    }

    /// Add a set of conflicts between txs
    fn add_conflicts(
        &mut self,
        conflicts: Conflicts,
    ) -> Result<(), MempoolUpdateError> {
        for (txid_lo, conflict_txids) in conflicts.iter() {
            let conflict_txids = OrdSet::from(conflict_txids);
            if conflict_txids.is_empty() {
                break;
            }
            self.txs.descendants_mut(*txid_lo).try_for_each(
                |descendant_info| {
                    let (_desc_tx, desc_info) = descendant_info?;
                    desc_info.conflicts_with = desc_info
                        .conflicts_with
                        .clone()
                        .union(conflict_txids.clone());
                    Result::<_, MissingDescendantError>::Ok(())
                },
            )?;
        }
        for (txid_hi, conflict_txids) in conflicts.iter_inverted() {
            let conflict_txids = OrdSet::from(conflict_txids);
            if conflict_txids.is_empty() {
                break;
            }
            self.txs.descendants_mut(txid_hi).try_for_each(
                |descendant_info| {
                    let (_desc_tx, desc_info) = descendant_info?;
                    desc_info.conflicts_with = desc_info
                        .conflicts_with
                        .clone()
                        .union(conflict_txids.clone());
                    Result::<_, MissingDescendantError>::Ok(())
                },
            )?;
        }
        Ok(())
    }

    /// Tweak the weight for a tx
    fn tweak_weight(
        &mut self,
        txid: Txid,
        tweak: i64,
    ) -> Result<(), MempoolRemoveError> {
        if tweak == 0 {
            return Ok(());
        }
        let Some((_, tx_info)) = self.txs.0.get_mut(&txid) else {
            return Ok(());
        };
        let modified_weight =
            saturating_add_signed_weight(tx_info.modified_weight, tweak);
        // weight amount that was actually tweaked. May have lower absolute
        // value due to saturating addition
        let tweaked_wu = if tweak >= 0 {
            (modified_weight - tx_info.modified_weight).to_wu() as i64
        } else {
            -((tx_info.modified_weight - modified_weight).to_wu() as i64)
        };
        tx_info.modified_weight = modified_weight;
        tx_info.descendant_modified_weight = saturating_add_signed_weight(
            tx_info.descendant_modified_weight,
            tweaked_wu,
        );
        self.txs.ancestors_mut(txid).try_for_each(|ancestor_info| {
            let (_, anc_info) = ancestor_info?;
            anc_info.descendant_modified_weight = saturating_add_signed_weight(
                anc_info.descendant_modified_weight,
                tweaked_wu,
            );
            Ok::<_, MempoolRemoveError>(())
        })?;
        self.txs
            .descendants_mut(txid)
            .try_for_each(|descendant_info| {
                let (desc_tx, desc_info) = descendant_info?;
                let desc_txid = desc_tx.compute_txid();
                let mut ancestor_fee_rate = FeeRate {
                    fee: desc_info.fees.ancestor,
                    vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
                };
                if !self
                    .by_ancestor_fee_rate
                    .remove(ancestor_fee_rate, desc_txid)
                {
                    let err =
                        MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
                    return Err(err.into());
                };
                desc_info.ancestor_modified_weight =
                    saturating_add_signed_weight(
                        desc_info.ancestor_modified_weight,
                        tweaked_wu,
                    );
                ancestor_fee_rate.vsize =
                    desc_info.ancestor_modified_weight.to_vbytes_ceil();
                self.by_ancestor_fee_rate
                    .insert(ancestor_fee_rate, desc_txid);
                Ok::<_, MempoolRemoveError>(())
            })?;
        Ok(())
    }

    /// choose txs for a block proposal, mutating the underlying mempool.
    /// If no weight limit is specified, or the specified weight exceeds
    /// `Weight::MAX_BLOCK`, then `Weight::MAX_BLOCK` will be used as the
    /// weight limit.
    fn propose_txs_mut(
        &mut self,
        weight_limit: Option<Weight>,
    ) -> Result<IndexSet<Txid>, MempoolRemoveError> {
        let mut res = IndexSet::new();
        let mut weight_remaining = weight_limit
            .unwrap_or(MAX_USABLE_BLOCK_WEIGHT)
            .min(MAX_USABLE_BLOCK_WEIGHT);
        tracing::debug!(%weight_remaining, "Selecting txs");
        loop {
            let Some((ancestor_fee_rate, txid)) = self
                .by_ancestor_fee_rate
                .iter_rev()
                .find(|(ancestor_fee_rate, _txid)| {
                    Weight::from_vb(ancestor_fee_rate.vsize).is_some_and(
                        |ancestors_weight| ancestors_weight <= weight_remaining,
                    )
                })
            else {
                break;
            };
            tracing::trace!(%txid, "Proposing tx with ancestors");
            // stack of txs to add
            let mut to_add = vec![(txid, false)];
            while let Some((txid, parents_visited)) = to_add.pop() {
                if parents_visited {
                    tracing::trace!(%txid, "Removing tx from mempool");
                    let (_tx, info) = self
                        .remove(&txid)?
                        .expect("missing tx in mempool when proposing txs");
                    res.insert(txid);
                    // Remove conflicts for the final tx
                    if to_add.is_empty() {
                        for conflict_txid in info.conflicts_with {
                            for (removed_txid, _removed_tx) in
                                self.remove_with_descendants(&conflict_txid)?
                            {
                                tracing::trace!(%txid, %removed_txid, "Removed tx from mempool due to conflict");
                            }
                        }
                    }
                } else {
                    let Some((_, info)) = self.txs.0.get(&txid) else {
                        tracing::warn!(%txid, "Missing tx in mempool when proposing txs, omitting from block template proposal");
                        continue;
                    };

                    to_add.push((txid, true));
                    to_add.extend(info.depends.iter().map(|dep| (*dep, false)))
                }
            }
            weight_remaining -= Weight::from_vb_unwrap(ancestor_fee_rate.vsize);
        }
        Ok(res)
    }

    pub fn propose_txs(
        &self,
        weight_limit: Option<Weight>,
    ) -> Result<Vec<BlockTemplateTransaction>, MempoolRemoveError> {
        let mut txs = self.clone().propose_txs_mut(weight_limit)?;
        let mut res = Vec::new();
        // build result in reverse order
        while let Some(txid) = txs.pop() {
            tracing::trace!(%txid, "Computing deps for tx");
            let mut depends = Vec::new();
            let mut ancestors = self.txs.ancestors(txid);
            while let Some((anc_txid, _, _)) = ancestors.next().transpose()? {
                let anc_idx = txs.get_index_of(&anc_txid).ok_or(
                    MissingAncestorError {
                        tx: txid,
                        missing: anc_txid,
                    },
                )?;
                depends.push(anc_idx as u32);
            }
            depends.sort();

            // TODO: Not sure if this is correct behavior. But avoid panics if we're
            // handling a transaction that we cannot find.
            let Some((tx, info)) = self.txs.0.get(&txid) else {
                tracing::warn!(%txid, "Missing tx in mempool when proposing txs, omitting from block template");
                continue;
            };

            let block_template_tx = BlockTemplateTransaction {
                data: bitcoin::consensus::serialize(tx),
                txid,
                hash: tx.compute_wtxid(),
                depends,
                fee: bitcoin::SignedAmount::from_sat(info.fees.base as i64),
                // FIXME: compute this
                sigops: None,
                weight: tx.weight().to_wu(),
            };
            res.push(block_template_tx);
        }
        res.reverse();
        Ok(res)
    }
}
