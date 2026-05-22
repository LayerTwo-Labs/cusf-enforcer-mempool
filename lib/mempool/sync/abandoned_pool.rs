//! Abandoned pool
//!
//! When mempool txs are removed, they become unavailable, and so any
//! transactions that require removed mempool txs in order to be validated are
//! placed into an 'abandoned' pool, which is expected to be empty at the end
//! of a successful mempool sync.
//! Transactions in the abandoned pool are not validated by the enforcer.
//! If a removed mempool tx becomes available again, transactions that depend
//! on it in the abandoned pool can be validated and added back into the
//! mempool.
//! When a transaction is evicted from the node's mempool, it may be removed
//! from the abandoned pool.

use std::collections::{HashMap, HashSet, VecDeque, hash_map};

use bitcoin::{Transaction, Txid};
use hashlink::{LinkedHashMap, LinkedHashSet};

#[derive(Debug)]
pub struct AbandonedTx {
    pub abandoned_parent_txids: HashSet<Txid>,
    pub absent_parent_txids: HashSet<Txid>,
    pub child_txids: LinkedHashSet<Txid>,
    pub tx: Transaction,
}

#[derive(Debug, Default)]
pub struct AbandonedPool {
    /// Map associating absent parent txs with child txs
    absent_parent_to_child_txids: HashMap<Txid, LinkedHashSet<Txid>>,
    /// Map associating available (not including abandoned) parent txs with
    /// child txs
    available_parent_to_child_txids: HashMap<Txid, LinkedHashSet<Txid>>,
    abandoned_txs: HashMap<Txid, AbandonedTx>,
}

impl AbandonedPool {
    pub fn is_empty(&self) -> bool {
        self.abandoned_txs.is_empty()
    }

    /// Returns `true` if the tx is present in the pool.
    pub fn contains(&self, txid: &Txid) -> bool {
        self.abandoned_txs.contains_key(txid)
    }

    /// Returns the existing value if the tx was already present in the
    /// pool.
    /// Provided absent parents are ignored if they are not present in the tx's
    /// inputs.
    pub fn insert<'a>(
        &'a mut self,
        tx: Transaction,
        absent_parent_txids: HashSet<Txid>,
    ) -> Option<&'a AbandonedTx> {
        let txid = tx.compute_txid();
        let mut this = self;
        // surprisingly, the borrow checker can't handle this, so we need
        // polonius
        polonius_the_crab::polonius!(
            |this| -> Option<&'polonius AbandonedTx> {
                if let Some(abandoned_tx) = this.abandoned_txs.get(&txid) {
                    polonius_the_crab::polonius_return!(Some(abandoned_tx))
                }
            }
        );
        let absent_maybe_parent_txids = absent_parent_txids;
        let mut abandoned_parent_txids = HashSet::new();
        let mut absent_parent_txids = HashSet::new();
        for txin in &tx.input {
            let parent_txid = &txin.previous_output.txid;
            if let Some(abandoned_parent) =
                this.abandoned_txs.get_mut(parent_txid)
            {
                abandoned_parent_txids.insert(*parent_txid);
                abandoned_parent.child_txids.replace(txid);
            } else if absent_maybe_parent_txids.contains(parent_txid) {
                absent_parent_txids.insert(*parent_txid);
                this.absent_parent_to_child_txids
                    .entry(*parent_txid)
                    .or_default()
                    .replace(txid);
            } else {
                this.available_parent_to_child_txids
                    .entry(*parent_txid)
                    .or_default()
                    .replace(txid);
            }
        }
        let mut child_txids = this
            .absent_parent_to_child_txids
            .remove(&txid)
            .unwrap_or_default();
        child_txids.extend(
            this.available_parent_to_child_txids
                .remove(&txid)
                .iter()
                .flatten(),
        );
        for child_txid in &child_txids {
            if let Some(child_tx) = this.abandoned_txs.get_mut(child_txid) {
                if !child_tx.absent_parent_txids.remove(&txid) {
                    tracing::warn!("missing entry")
                };
                child_tx.abandoned_parent_txids.insert(txid);
            } else {
                tracing::warn!("missing entry")
            }
        }
        let abandoned_tx = AbandonedTx {
            abandoned_parent_txids,
            absent_parent_txids,
            child_txids,
            tx,
        };
        this.abandoned_txs.insert(txid, abandoned_tx);
        None
    }

    /// Removes a tx from the abandoned pool.
    /// The tx will be marked as unavailable, so any child txs in the pool will
    /// remain in the pool.
    pub fn remove(&mut self, txid: &Txid) -> Option<AbandonedTx> {
        let (txid, res) = self.abandoned_txs.remove_entry(txid)?;
        let AbandonedTx {
            abandoned_parent_txids: _,
            absent_parent_txids: _,
            child_txids,
            tx,
        } = &res;
        let parent_txids: LinkedHashSet<_> = tx
            .input
            .iter()
            .map(|txin| &txin.previous_output.txid)
            .collect();
        for parent_txid in parent_txids {
            if let Some(abandoned_parent) =
                self.abandoned_txs.get_mut(parent_txid)
            {
                abandoned_parent.child_txids.remove(&txid);
            } else if let hash_map::Entry::Occupied(mut entry) =
                self.absent_parent_to_child_txids.entry(*parent_txid)
            {
                let child_txids = entry.get_mut();
                if !child_txids.remove(&txid) {
                    tracing::warn!("missing entry");
                };
                if child_txids.is_empty() {
                    entry.remove();
                }
            } else if let hash_map::Entry::Occupied(mut entry) =
                self.available_parent_to_child_txids.entry(*parent_txid)
            {
                let child_txids = entry.get_mut();
                if !child_txids.remove(&txid) {
                    tracing::warn!("missing entry");
                };
                if child_txids.is_empty() {
                    entry.remove();
                }
            } else {
                tracing::warn!("missing entry");
            }
        }
        if !child_txids.is_empty() {
            self.absent_parent_to_child_txids
                .insert(txid, child_txids.clone());
        }
        for child_txid in child_txids {
            if let Some(child_tx) = self.abandoned_txs.get_mut(child_txid) {
                if !child_tx.abandoned_parent_txids.remove(&txid) {
                    tracing::warn!("missing entry");
                }
                child_tx.absent_parent_txids.insert(txid);
            } else {
                tracing::warn!("missing entry");
            }
        }
        Some(res)
    }

    /// Remove all descendants of a tx, eg. when rejecting a parent tx.
    /// The tx is removed, if present.
    pub fn remove_descendant_txs(
        &mut self,
        txid: &Txid,
    ) -> LinkedHashMap<Txid, Transaction> {
        let mut res = LinkedHashMap::new();
        if let Some(abandoned_tx) = self.remove(txid) {
            res.replace(*txid, abandoned_tx.tx);
        }
        let mut stack = LinkedHashSet::new();
        stack.extend(
            self.absent_parent_to_child_txids
                .remove(txid)
                .iter()
                .flatten(),
        );
        stack.extend(
            self.available_parent_to_child_txids
                .remove(txid)
                .iter()
                .flatten(),
        );
        while let Some(txid) = stack.pop_front() {
            if let Some(abandoned_tx) = self.remove(&txid) {
                res.replace(txid, abandoned_tx.tx);
            }
            stack.extend(
                self.absent_parent_to_child_txids
                    .remove(&txid)
                    .iter()
                    .flatten(),
            );
        }
        res
    }

    /// Restore descendant txs of the now-available tx
    pub fn restore_descendant_txs(
        &mut self,
        txid: &Txid,
    ) -> LinkedHashMap<Txid, Transaction> {
        let mut res = LinkedHashMap::new();
        let mut stack = VecDeque::new();
        let Some(child_txids) = self.absent_parent_to_child_txids.remove(txid)
        else {
            return res;
        };
        for child_txid in child_txids {
            match self.abandoned_txs.entry(child_txid) {
                hash_map::Entry::Occupied(mut entry) => {
                    let child_tx = entry.get_mut();
                    if !child_tx.absent_parent_txids.remove(txid) {
                        tracing::warn!("missing entry");
                    }
                    if child_tx.absent_parent_txids.is_empty()
                        && child_tx.abandoned_parent_txids.is_empty()
                    {
                        let (child_txid, child_tx) = entry.remove_entry();
                        res.replace(child_txid, child_tx.tx);
                        stack.push_back((child_txid, child_tx.child_txids));
                    }
                }
                hash_map::Entry::Vacant(_) => {
                    tracing::warn!("missing entry");
                }
            }
        }
        while let Some((txid, child_txids)) = stack.pop_front() {
            for child_txid in child_txids {
                match self.abandoned_txs.entry(child_txid) {
                    hash_map::Entry::Occupied(mut entry) => {
                        let child_tx = entry.get_mut();
                        if !child_tx.abandoned_parent_txids.remove(&txid) {
                            tracing::warn!("missing entry");
                        }
                        if child_tx.absent_parent_txids.is_empty()
                            && child_tx.abandoned_parent_txids.is_empty()
                        {
                            let (child_txid, child_tx) = entry.remove_entry();
                            res.replace(child_txid, child_tx.tx);
                            stack.push_back((child_txid, child_tx.child_txids));
                        }
                    }
                    hash_map::Entry::Vacant(_) => {
                        tracing::warn!("missing entry");
                    }
                }
            }
        }
        res
    }
}
