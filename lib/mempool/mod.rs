use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
};

use bip300301::client::{
    BlockTemplateTransaction, GetBlockClient, RawMempoolTxFees,
};
use bitcoin::{BlockHash, Target, Transaction, Txid, Weight};
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
    size: u64,
}

impl Ord for FeeRate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // (self.fee / self.size) > (other.fee / other.size) ==>
        // (self.fee * other.size) > (other.fee * self.size)
        let lhs = self.fee as u128 * other.size as u128;
        let rhs = other.fee as u128 * self.size as u128;
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
    pub ancestor_size: u64,
    pub bip125_replaceable: bool,
    pub depends: OrdSet<Txid>,
    pub descendant_size: u64,
    pub fees: RawMempoolTxFees,
    pub spent_by: OrdSet<Txid>,
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
    blocks: imbl::HashMap<BlockHash, bip300301::client::Block<true>>,
    block_heights: imbl::HashMap<u64, BlockHash>,

    // Used for fetching blocks that aren't in our local cache
    main_client: jsonrpsee::http_client::HttpClient,
}

impl Chain {
    // Iterate over blocks from tip towards genesis.
    // Not all history is guaranteed to exist, so this iterator might return
    // `None` before the genesis block.
    #[allow(dead_code)]
    fn iter(&self) -> impl Iterator<Item = &bip300301::client::Block<true>> {
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

    /// Find a block by its height
    /// Returns None if no block with the given height exists in the chain
    pub async fn get_block_by_height(
        &self,
        height: u64,
    ) -> Result<bip300301::client::Block<true>, jsonrpsee::core::client::Error>
    {
        if let Some(hash) = self.block_heights.get(&height) {
            tracing::debug!(
                "Found block at height {} in cache: {}",
                height,
                hash
            );
            return Ok(self.blocks[hash].clone());
        }

        tracing::debug!("Fetching block at height {} from main client", height);

        let block_hash = self.main_client.get_block_hash(height).await?;
        let block = self
            .main_client
            .get_block(block_hash, bip300301::client::U8Witness::<2>)
            .await?;

        Ok(block)
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

// MUST be cheap to clone so that constructing block templates is cheap
#[derive(Clone, Debug)]
pub struct Mempool {
    by_ancestor_fee_rate: ByAncestorFeeRate,
    chain: Chain,
    /// Map of txs (which may not be in the mempool) to their direct child txs,
    /// which MUST be in the mempool
    tx_childs: TxChilds,
    txs: MempoolTxs,
}

impl Mempool {
    fn new(
        prev_blockhash: BlockHash,
        main_client: jsonrpsee::http_client::HttpClient,
    ) -> Self {
        let chain = Chain {
            tip: prev_blockhash,
            blocks: imbl::HashMap::new(),
            block_heights: imbl::HashMap::new(),
            main_client,
        };
        Self {
            by_ancestor_fee_rate: ByAncestorFeeRate::default(),
            chain,
            tx_childs: TxChilds::default(),
            txs: MempoolTxs::default(),
        }
    }

    pub fn tip(&self) -> &bip300301::client::Block<true> {
        &self.chain.blocks[&self.chain.tip]
    }

    pub async fn next_target(
        &self,
        params: &bitcoin::consensus::Params,
    ) -> Target {
        let last_block = &self.chain.blocks[&self.chain.tip];

        // If there's no retargeting on the network, that's great
        if params.no_pow_retargeting {
            return last_block.compact_target.into();
        }

        // We need to determine if we're on the boundary of a difficulty period
        // If not, just return the current target
        if ((last_block.height as u64) + 1)
            % params.difficulty_adjustment_interval()
            != 0
        {
            return last_block.compact_target.into();
        }

        // Difficulty adjustment!
        let last_block_header = bitcoin::block::Header {
            version: last_block.version,
            prev_blockhash: last_block.previousblockhash.unwrap(),
            merkle_root: last_block.merkleroot,
            time: last_block.time,
            bits: last_block.compact_target,
            nonce: last_block.nonce,
        };

        // Get the block from the beginning of this difficulty adjustment period
        let boundary_height = last_block.height as u64
            - params.difficulty_adjustment_interval()
            // +1 because we want the block at the beginning of the period, not the 
            // first block of the /previous/ period
            + 1;

        // First try to get the block from cache
        let boundary = match self
            .chain
            .get_block_by_height(boundary_height)
            .await
        {
            Ok(block) => block,
            Err(err) => {
                // If we couldn't fetch the boundary block, we have to fall back to the current target
                tracing::warn!(
                    error = ?err,
                    "Unable to find block at height {} for difficulty adjustment, using current target",
                    boundary_height
                );
                return last_block.compact_target.into();
            }
        };

        let boundary_header = bitcoin::block::Header {
            version: boundary.version,
            prev_blockhash: boundary.previousblockhash.unwrap(),
            merkle_root: boundary.merkleroot,
            time: boundary.time,
            bits: boundary.compact_target,
            nonce: boundary.nonce,
        };

        let compact_target =
            bitcoin::CompactTarget::from_header_difficulty_adjustment(
                boundary_header,
                last_block_header,
                params,
            );

        tracing::debug!(
            last_block_height = last_block.height,
            last_block_difficulty_adjustment_height = boundary_height,
            "determined next PoW target: {:x} -> {:x}",
            last_block_header.bits.to_consensus(),
            compact_target.to_consensus()
        );

        compact_target.into()
    }

    /// Insert a tx into the mempool
    pub fn insert(
        &mut self,
        tx: Transaction,
        fee: u64,
    ) -> Result<Option<TxInfo>, MempoolInsertError> {
        let txid = tx.compute_txid();
        // initially incorrect, must be computed after insertion
        let mut ancestor_fees = fee;
        // initially incorrect, must be computed after insertion
        let mut descendant_fees = fee;
        let modified_fee = fee;
        let vsize = tx.vsize() as u64;
        // initially incorrect, must be computed after insertion
        let mut ancestor_size = vsize;
        // initially incorrect, must be computed after insertion
        let mut descendant_size = vsize;
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
        }
        let spent_by = if let Some(childs) = self.tx_childs.0.get(&txid) {
            OrdSet::from_iter(childs.iter().copied())
        } else {
            OrdSet::new()
        };
        let info = TxInfo {
            ancestor_size,
            bip125_replaceable: tx.is_explicitly_rbf(),
            depends,
            descendant_size,
            fees: RawMempoolTxFees {
                ancestor: ancestor_fees,
                base: fee,
                descendant: descendant_fees,
                modified: modified_fee,
            },
            spent_by,
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
            ancestor_size += ancestor_tx.vsize() as u64;
            ancestor_fees += ancestor_info.fees.modified;
            ancestor_info.descendant_size += vsize;
            ancestor_info.fees.descendant += modified_fee;
            Result::<_, MempoolInsertError>::Ok(())
        })?;
        self.txs.descendants_mut(txid).skip(1).try_for_each(
            |descendant_info| {
                let (descendant_tx, descendant_info) = descendant_info?;
                descendant_size += descendant_tx.vsize() as u64;
                descendant_fees += descendant_info.fees.modified;
                descendant_info.ancestor_size += vsize;
                descendant_info.fees.ancestor += modified_fee;
                Result::<_, MempoolInsertError>::Ok(())
            },
        )?;
        let (_, info) = self.txs.0.get_mut(&txid).unwrap();
        info.fees.ancestor = ancestor_fees;
        info.fees.descendant = descendant_fees;
        info.ancestor_size = ancestor_size;
        info.descendant_size = descendant_size;
        let ancestor_fee_rate = FeeRate {
            fee: ancestor_fees,
            size: ancestor_size,
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
        let ancestor_size = info.ancestor_size;
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
                size: desc_info.ancestor_size,
            };
            let desc_txid = desc_tx.compute_txid();
            if !self
                .by_ancestor_fee_rate
                .remove(ancestor_fee_rate, desc_txid)
            {
                let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
                return Err(err.into());
            };
            desc_info.ancestor_size -= vsize;
            desc_info.fees.ancestor -= fees.modified;
            let ancestor_fee_rate = FeeRate {
                fee: desc_info.fees.ancestor,
                size: desc_info.ancestor_size,
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
            anc_info.descendant_size -= vsize;
            anc_info.fees.descendant -= fees.modified;
            let anc_txid = anc_tx.compute_txid();
            // FIXME: remove
            tracing::debug!("removing {txid} as a spender of {anc_txid}");
            anc_info.spent_by.remove(txid);
            Result::<_, MempoolRemoveError>::Ok(())
        })?;
        let ancestor_fee_rate = FeeRate {
            fee: fees.ancestor,
            size: ancestor_size,
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

    /// choose txs for a block proposal, mutating the underlying mempool
    fn propose_txs_mut(
        &mut self,
    ) -> Result<IndexSet<Txid>, MempoolRemoveError> {
        let mut res = IndexSet::new();
        let mut total_size = 0;
        loop {
            let Some((ancestor_fee_rate, txid)) = self
                .by_ancestor_fee_rate
                .iter_rev()
                .find(|(ancestor_fee_rate, _txid)| {
                    let total_weight =
                        Weight::from_vb(total_size + ancestor_fee_rate.size);
                    total_weight
                        .is_some_and(|weight| weight < Weight::MAX_BLOCK)
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
                    let (_tx, _info) = self
                        .remove(&txid)?
                        .expect("missing tx in mempool when proposing txs");
                    res.insert(txid);
                } else {
                    let (_tx, info) = &self.txs.0[&txid];
                    to_add.push((txid, true));
                    to_add.extend(info.depends.iter().map(|dep| (*dep, false)))
                }
            }
            total_size += ancestor_fee_rate.size;
        }
        Ok(res)
    }

    pub fn propose_txs(
        &self,
    ) -> Result<Vec<BlockTemplateTransaction>, MempoolRemoveError> {
        let mut txs = self.clone().propose_txs_mut()?;
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
            let (tx, info) = &self.txs.0[&txid];
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

#[jsonrpsee::proc_macros::rpc(client)]
trait HashFetcher {
    #[method(name = "getblockhash")]
    async fn get_block_hash(
        &self,
        height: u64,
    ) -> Result<bitcoin::BlockHash, jsonrpsee::core::Error>;
}
