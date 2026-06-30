//! Initial mempool sync
//!
//! During initial mempool sync, we maintain both a filtered and unfiltered
//! mempool.
//! This allows us to correctly mirror the node's mempool, whilst also
//! constructing a mempool that enforces a CUSF enforcer's validation rules.

use core::future::Future;
use std::{
    borrow::BorrowMut,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use bitcoin::{Amount, BlockHash, OutPoint, Transaction, TxIn, Txid, Weight};
use bitcoin_jsonrpsee::{
    bitcoin::hashes::Hash as _,
    client::{BoolWitness, GetRawMempoolClient as _, RawMempoolWithSequence},
    jsonrpsee::core::ClientError as JsonRpcError,
};
use educe::Educe;
use futures::{
    FutureExt as _, StreamExt as _,
    future::{BoxFuture, FusedFuture},
};
use hashlink::LinkedHashSet;
use thiserror::Error;
use tokio::sync::RwLock as TokioRwLock;
use tracing::Instrument;

use crate::{
    cusf_enforcer::{
        self, ConnectBlockAction, CusfEnforcer, DisconnectBlockAction,
    },
    mempool::{
        Mempool, MempoolInsertError, MempoolRemoveError, MempoolUpdateError,
        sync::{
            ApplySyncActionResult, ApplySyncActionTimeoutError,
            BatchedResponseItem, CombinedStream, CombinedStreamItem,
            RequestError, RequestItem, RequestQueue, ResponseItem, SyncAction,
            SyncActionQueue, abandoned_pool::AbandonedPool,
            apply_sync_action_timeout, batched_request,
        },
    },
    zmq::{
        BlockHashEvent, BlockHashMessage, SequenceMessage, SequenceStreamError,
        TxHashEvent, TxHashMessage,
    },
};

#[derive(Debug)]
struct UnfilteredMempool {
    tip: BlockHash,
    txs: HashSet<Txid>,
}

#[derive(Educe)]
#[educe(Debug(bound(cusf_enforcer::Error<Enforcer>: std::fmt::Debug)))]
#[derive(Error)]
pub enum SyncTaskError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error(transparent)]
    ApplySyncActionTimeout(#[from] ApplySyncActionTimeoutError),
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error(
        "disconnected block tip ({}) does not match unfiltered mempool ({})",
        .disconnected_tip,
        .unfiltered_mempool_tip,
    )]
    DisconnectTipMismatch {
        disconnected_tip: BlockHash,
        unfiltered_mempool_tip: BlockHash,
    },
    #[error(transparent)]
    CusfEnforcer(#[from] cusf_enforcer::Error<Enforcer>),
    #[error("Failed to decode block: `{block_hash}`")]
    DecodeBlock {
        block_hash: BlockHash,
        source: bitcoin::consensus::encode::Error,
    },
    #[error("Fee overflow")]
    FeeOverflow,
    #[error("Missing first message with mempool sequence: {0}")]
    FirstMempoolSequence(u64),
    #[error(transparent)]
    InitialSyncEnforcer(#[from] cusf_enforcer::InitialSyncError<Enforcer>),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error(transparent)]
    MempoolUpdate(#[from] MempoolUpdateError),
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
    // TODO - this is not really an error...
    #[error("Sync was stopped")]
    Shutdown,
    #[error(
        "unexpected tip ({}) in unfiltered mempool, expected ({})",
        .tip,
        .expected,
    )]
    UnexpectedTipUnfilteredMempool { tip: BlockHash, expected: BlockHash },
    #[error("missing tx ({0}) in unfiltered mempool")]
    UnfilteredMempoolMissingTx(Txid),
    #[error("Value in overflow")]
    ValueInOverflow,
    #[error("Value out overflow")]
    ValueOutOverflow,
}

struct MempoolSyncInner<Enforcer> {
    abandoned_pool: AbandonedPool,
    enforcer: Enforcer,
    mempool: Mempool,
    unfiltered_mempool: UnfilteredMempool,
}

#[derive(Debug)]
pub struct SyncState {
    action_queue: SyncActionQueue,
    blocks_needed: LinkedHashSet<BlockHash>,
    /// Drop messages with lower mempool sequences.
    /// Set to None after encountering this mempool sequence ID.
    /// Return an error if higher sequence is encountered.
    first_mempool_sequence: Option<u64>,
    /// Txs rejected by the CUSF enforcer
    rejected_txs: HashSet<Txid>,
    request_queue: RequestQueue,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
    txs_needed: LinkedHashSet<Txid>,
    unavailable_txs: HashSet<Txid>,
}

/// Sync state, mutably borrowed while applying an action from the queue
struct SyncStateBorrowedMut<'a> {
    blocks_needed: &'a mut LinkedHashSet<BlockHash>,
    /// Txs rejected by the CUSF enforcer
    rejected_txs: &'a mut HashSet<Txid>,
    request_queue: &'a RequestQueue,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: &'a mut HashMap<Txid, Transaction>,
    txs_needed: &'a mut LinkedHashSet<Txid>,
    unavailable_txs: &'a mut HashSet<Txid>,
}

pub struct MempoolSyncing<Enforcer> {
    inner: MempoolSyncInner<Enforcer>,
    sync_state: SyncState,
}

struct MempoolSyncingBorrowed<'a, Enforcer> {
    inner: &'a MempoolSyncInner<Enforcer>,
    sync_state: &'a SyncState,
}

impl<Enforcer> MempoolSyncingBorrowed<'_, Enforcer> {
    fn is_synced(&self) -> bool {
        self.inner.abandoned_pool.is_empty()
            && self.sync_state.blocks_needed.is_empty()
            && self.sync_state.txs_needed.is_empty()
            && self.sync_state.action_queue.is_empty()
            && self.inner.mempool.chain.tip == self.inner.unfiltered_mempool.tip
    }
}

// TODO: move txs_needed / request_queue handling out
async fn connect_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let prev_blockhash =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    if prev_blockhash != inner.unfiltered_mempool.tip {
        if block.hash == inner.unfiltered_mempool.tip {
            // Ignore, block has already been applied
            return Ok(());
        }
        // A subscriber that joins while bitcoind is still flushing its ZMQ
        // notification queue receives connect events for blocks that the
        // initial-sync tip snapshot already covers.
        if block.confirmations > 0
            && let Some(tip_block) = inner
                .mempool
                .chain
                .blocks
                .get(&inner.unfiltered_mempool.tip)
            && block.height < tip_block.height
        {
            tracing::debug!(
                block_hash = %block.hash,
                block_height = block.height,
                tip = %inner.unfiltered_mempool.tip,
                "Ignoring stale block connect below current tip"
            );
            return Ok(());
        }
        return Err(SyncTaskError::UnexpectedTipUnfilteredMempool {
            tip: inner.unfiltered_mempool.tip,
            expected: prev_blockhash,
        });
    }
    for tx_info in &block.tx {
        inner.unfiltered_mempool.txs.remove(&tx_info.txid);
    }
    inner.unfiltered_mempool.tip = block.hash;
    if prev_blockhash != inner.mempool.chain.tip {
        tracing::debug!(
            block_hash = %block.hash,
            mempool_tip = %inner.mempool.chain.tip,
            %prev_blockhash,
            "Rejecting block due to rejected parent"
        );
        sync_state
            .request_queue
            .push_front(RequestItem::RejectBlock(block.hash));
        return Ok(());
    }
    let block_decoded =
        block.try_into().map_err(|err| SyncTaskError::DecodeBlock {
            block_hash: block.hash,
            source: err,
        })?;
    match inner
        .enforcer
        .borrow_mut()
        .connect_block(&block_decoded)
        .await
        .map_err(cusf_enforcer::Error::ConnectBlock)?
    {
        ConnectBlockAction::Accept { remove_mempool_txs } => {
            for tx in block_decoded.txdata {
                let txid = tx.compute_txid();
                let _removed: Option<_> = inner.mempool.remove(&txid)?;
                sync_state.txs_needed.remove(&txid);
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(txid, true));
                sync_state.tx_cache.insert(txid, tx);
            }
            for txid in remove_mempool_txs {
                inner.mempool.remove_with_descendants(&txid)?;
            }
            inner.mempool.chain.tip = block.hash;
        }
        ConnectBlockAction::Reject => {
            sync_state
                .request_queue
                .push_front(RequestItem::RejectBlock(block.hash));
        }
    };
    Ok(())
}

/// Returns `false` without applying the disconnect if the parent block must
/// first be fetched into `chain.blocks`. The caller should retry once the
/// parent block response has been handled.
async fn handle_disconnected_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    blocks_needed: &mut LinkedHashSet<BlockHash>,
    request_queue: &RequestQueue,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<bool, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let prev_blockhash =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    if inner.unfiltered_mempool.tip != block.hash {
        return Err(SyncTaskError::DisconnectTipMismatch {
            disconnected_tip: block.hash,
            unfiltered_mempool_tip: inner.unfiltered_mempool.tip,
        });
    }
    if inner.mempool.chain.tip == block.hash {
        // Rolling the tip back requires the parent block to be present in
        // `chain.blocks` because `Mempool::tip` indexes into it. The parent is
        // absent if it predates the initial sync, which fetches only the
        // sync-time tip itself.
        if block.previousblockhash.is_some()
            && !inner.mempool.chain.blocks.contains_key(&prev_blockhash)
        {
            if !blocks_needed.contains(&prev_blockhash) {
                tracing::debug!(
                    block_hash = %block.hash,
                    %prev_blockhash,
                    "Requesting parent block before disconnect"
                );
                blocks_needed.replace(prev_blockhash);
                request_queue.push_front(RequestItem::Block(prev_blockhash));
            }
            return Ok(false);
        }
        let DisconnectBlockAction { remove_mempool_txs } = inner
            .enforcer
            .borrow_mut()
            .disconnect_block(block.hash)
            .await
            .map_err(cusf_enforcer::Error::DisconnectBlock)?;
        for txid in remove_mempool_txs {
            inner.mempool.remove_with_descendants(&txid)?;
        }
        inner.mempool.chain.tip = prev_blockhash;
    }
    inner.unfiltered_mempool.tip = prev_blockhash;
    Ok(true)
}

fn handle_block_hash_msg<Enforcer>(
    inner: &MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    block_hash_msg: BlockHashMessage,
) {
    let BlockHashMessage {
        block_hash,
        event: _,
        ..
    } = block_hash_msg;
    if !inner.mempool.chain.blocks.contains_key(&block_hash) {
        tracing::trace!(%block_hash, "Adding block to req queue");
        sync_state.blocks_needed.replace(block_hash);
        sync_state
            .request_queue
            .push_back(RequestItem::Block(block_hash));
    }
}

fn handle_tx_hash_msg<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    tx_hash_msg: TxHashMessage,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let TxHashMessage {
        txid,
        event,
        mempool_seq,
        zmq_seq: _,
    } = tx_hash_msg;
    if let Some(first_mempool_seq) = sync_state.first_mempool_sequence {
        match mempool_seq.cmp(&first_mempool_seq) {
            Ordering::Less => {
                // Ignore
                return Ok(());
            }
            Ordering::Equal => {
                sync_state.first_mempool_sequence = None;
            }
            Ordering::Greater => {
                // TODO: ignore for now
                /*
                return Err(SyncTaskError::FirstMempoolSequence(
                    first_mempool_seq,
                ));
                */
                tracing::warn!(
                    %mempool_seq,
                    "Expected first mempool seq to be <= {first_mempool_seq}"
                );
                sync_state.first_mempool_sequence = None;
            }
        }
    }
    match event {
        TxHashEvent::Added => {
            if sync_state.tx_cache.contains_key(&txid) {
                // Nothing to do
                return Ok(());
            }
            sync_state.txs_needed.replace(txid);
            if sync_state.unavailable_txs.remove(&txid) {
                // Push to the front of the request queue, so that previously
                // abandoned txs can be added to the mempool
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(txid, true));
                let restored_txs =
                    inner.abandoned_pool.restore_descendant_txs(&txid);
                for (restored_txid, restored_tx) in
                    restored_txs.into_iter().rev()
                {
                    sync_state.tx_cache.insert(restored_txid, restored_tx);
                    // Push to the front of the action queue, so that
                    // previously abandoned txs can be added into the mempool
                    sync_state
                        .action_queue
                        .push_front(SyncAction::InsertTx(restored_txid));
                }
            } else {
                sync_state
                    .request_queue
                    .push_back(RequestItem::Tx(txid, true));
            }
            tracing::trace!(%txid, "Added tx to req queue");
        }
        TxHashEvent::Removed => {
            tracing::trace!(%txid, "Removed tx from req queue");
            sync_state.txs_needed.remove(&txid);
            sync_state.unavailable_txs.insert(txid);
            sync_state
                .request_queue
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    Ok(())
}

fn handle_seq_message<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    seq_msg: SequenceMessage,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(block_hash_msg) => {
            let () = handle_block_hash_msg(inner, sync_state, block_hash_msg);
        }
        SequenceMessage::TxHash(tx_hash_msg) => {
            let () = handle_tx_hash_msg(inner, sync_state, tx_hash_msg)?;
        }
    }
    sync_state
        .action_queue
        .push_back(SyncAction::SequenceMessage(seq_msg));
    Ok(())
}

async fn handle_resp_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp_block: bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    sync_state.blocks_needed.remove(&resp_block.hash);
    for tx_info in resp_block.tx.iter().rev() {
        sync_state.unavailable_txs.remove(&tx_info.txid);
        for (restored_txid, restored_tx) in inner
            .abandoned_pool
            .restore_descendant_txs(&tx_info.txid)
            .into_iter()
            .rev()
        {
            sync_state.tx_cache.insert(restored_txid, restored_tx);
            sync_state
                .action_queue
                .push_front(SyncAction::InsertTx(restored_txid));
        }
    }
    match sync_state
        .action_queue
        .front()
        .as_ref()
        .map(|front| &front.action)
    {
        Some(SyncAction::SequenceMessage(SequenceMessage::BlockHash(
            BlockHashMessage {
                block_hash,
                event: BlockHashEvent::Connected,
                ..
            },
        ))) if *block_hash == resp_block.hash => {
            {
                let sync_state = SyncStateBorrowedMut {
                    blocks_needed: &mut sync_state.blocks_needed,
                    rejected_txs: &mut sync_state.rejected_txs,
                    request_queue: &sync_state.request_queue,
                    tx_cache: &mut sync_state.tx_cache,
                    txs_needed: &mut sync_state.txs_needed,
                    unavailable_txs: &mut sync_state.unavailable_txs,
                };
                let () = connect_block(inner, sync_state, &resp_block).await?;
            };
            sync_state.action_queue.pop_front();
        }
        Some(SyncAction::SequenceMessage(SequenceMessage::BlockHash(
            BlockHashMessage {
                block_hash,
                event: BlockHashEvent::Disconnected,
                ..
            },
        ))) if *block_hash == resp_block.hash
            && inner.mempool.chain.tip == resp_block.hash =>
        {
            if handle_disconnected_block(
                inner,
                &mut sync_state.blocks_needed,
                &sync_state.request_queue,
                &resp_block,
            )
            .await?
            {
                sync_state.action_queue.pop_front();
            }
        }
        Some(_) | None => (),
    }
    inner
        .mempool
        .chain
        .blocks
        .insert(resp_block.hash, resp_block);
    Ok(())
}

fn handle_resp_tx(sync_state: &mut SyncState, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.txs_needed.remove(&txid);
    sync_state.tx_cache.insert(txid, tx);
}

/// First rejected parent, if any were rejected; otherwise
/// Required parent txs, if any must be requested; otherwise
/// Unavailable parent txs, if any are not available; otherwise
/// Abandoned parent txs, if any are abandoned; otherwise all parent txs
enum ParentTxsResult<'a> {
    /// First rejected parent tx, by vin
    Rejected(Txid),
    /// Required parent txs, sorted by vin
    Required(LinkedHashSet<Txid>),
    /// Unavailable parent txs, sorted by vin
    Unavailable(LinkedHashSet<Txid>),
    /// Abandoned parent txs, sorted by vin
    Abandoned(LinkedHashSet<Txid>),
    Available(HashMap<Txid, &'a Transaction>),
}

fn try_get_parent_txs_from_caches<'a>(
    abandoned_pool: &'a AbandonedPool,
    mempool: &'a Mempool,
    rejected_txs: &'a HashSet<Txid>,
    tx_cache: &'a HashMap<Txid, Transaction>,
    unavailable_txs: &'a HashSet<Txid>,
    vin: &[TxIn],
) -> ParentTxsResult<'a> {
    let mut abandoned_input_txs = LinkedHashSet::new();
    let mut input_txs_needed = LinkedHashSet::new();
    let mut unavailable_input_txs = LinkedHashSet::new();
    let mut input_txs = HashMap::<Txid, &Transaction>::new();
    for input in vin {
        let OutPoint {
            txid: input_txid,
            vout: _,
        } = input.previous_output;
        if rejected_txs.contains(&input_txid) {
            return ParentTxsResult::Rejected(input_txid);
        } else if let Some(input_tx) = tx_cache.get(&input_txid) {
            input_txs.insert(input_txid, input_tx);
        } else if let Some((input_tx, _)) = mempool.txs.0.get(&input_txid) {
            input_txs.insert(input_txid, input_tx);
        } else if abandoned_pool.contains(&input_txid) {
            abandoned_input_txs.replace(input_txid);
        } else if unavailable_txs.contains(&input_txid) {
            unavailable_input_txs.replace(input_txid);
        } else {
            input_txs_needed.replace(input_txid);
        }
    }
    if !input_txs_needed.is_empty() {
        ParentTxsResult::Required(input_txs_needed)
    } else if !unavailable_input_txs.is_empty() {
        ParentTxsResult::Unavailable(unavailable_input_txs)
    } else if !abandoned_input_txs.is_empty() {
        ParentTxsResult::Abandoned(abandoned_input_txs)
    } else {
        ParentTxsResult::Available(input_txs)
    }
}

fn fee_delta<Enforcer>(
    tx: &Transaction,
    input_txs: &HashMap<Txid, &Transaction>,
) -> Result<bitcoin::Amount, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let mut value_in = Amount::ZERO;
    for input in &tx.input {
        let OutPoint {
            txid: input_txid,
            vout,
        } = &input.previous_output;
        let value = input_txs[input_txid].output[*vout as usize].value;
        value_in = value_in
            .checked_add(value)
            .ok_or(SyncTaskError::ValueInOverflow)?;
    }

    let mut value_out = Amount::ZERO;
    for output in &tx.output {
        value_out = value_out
            .checked_add(output.value)
            .ok_or(SyncTaskError::ValueOutOverflow)?;
    }
    let fee_delta = value_in
        .checked_sub(value_out)
        .ok_or(SyncTaskError::FeeOverflow)?;
    Ok(fee_delta)
}

// returns `true` if the tx was added to the mempool or abandoned pool
// successfully, was already marked unavailable, or was rejected.
// returns `false` if the tx or the tx's parent txs are required.
fn try_add_tx_from_caches<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    txid: Txid,
) -> Result<bool, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    // The initial-sync RPC snapshot and the live ZMQ `sequence` stream are
    // reconciled together, so the same txid can be queued for insertion via
    // both paths (e.g. the snapshot `InsertTx` and a buffered `Added`
    // sequence message). Inserting a tx already in the mempool would make
    // `Mempool::insert` fail with `TxAlreadyExists` and abort the whole sync
    // task, so treat an already-present tx as a successfully-applied no-op.
    if inner.mempool.txs.0.contains_key(&txid) {
        inner.unfiltered_mempool.txs.insert(txid);
        return Ok(true);
    }
    let Some(tx) = sync_state.tx_cache.get(&txid) else {
        if sync_state.unavailable_txs.contains(&txid) {
            inner.unfiltered_mempool.txs.insert(txid);
            return Ok(true);
        } else {
            sync_state
                .request_queue
                .push_front(RequestItem::Tx(txid, true));
            return Ok(false);
        }
    };
    match try_get_parent_txs_from_caches(
        &inner.abandoned_pool,
        &inner.mempool,
        sync_state.rejected_txs,
        sync_state.tx_cache,
        sync_state.unavailable_txs,
        &tx.input,
    ) {
        ParentTxsResult::Rejected(rejected_parent) => {
            // Reject tx
            tracing::trace!(
                %txid,
                %rejected_parent,
                "rejecting tx: rejected parent",
            );
            sync_state.rejected_txs.insert(txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(txid));
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(true)
        }
        ParentTxsResult::Unavailable(unavailable_parents) => {
            let tx = sync_state
                .tx_cache
                .remove(&txid)
                .expect("tx should be present in tx cache");
            // TX is abandoned, add to abandoned pool
            inner
                .abandoned_pool
                .insert(tx, unavailable_parents.into_iter().collect());
            tracing::trace!(%txid, "added tx to abandoned pool");
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(true)
        }
        ParentTxsResult::Required(required_parents) => {
            for input_txid in required_parents.into_iter().rev() {
                sync_state.txs_needed.replace(input_txid);
                sync_state.txs_needed.to_front(&input_txid);
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
            Ok(false)
        }
        ParentTxsResult::Abandoned(_abandoned_parents) => {
            let tx = sync_state
                .tx_cache
                .remove(&txid)
                .expect("tx should be present in tx cache");
            inner.abandoned_pool.insert(tx, HashSet::new());
            tracing::trace!(%txid, "added tx to abandoned pool");
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(true)
        }
        ParentTxsResult::Available(parent_txs) => {
            let fee_delta = fee_delta(tx, &parent_txs)?;
            match inner
                .enforcer
                .borrow_mut()
                .accept_tx(tx, &parent_txs)
                .map_err(cusf_enforcer::Error::AcceptTx)?
            {
                cusf_enforcer::TxAcceptAction::Accept {
                    conflicts_with,
                    weight_tweak,
                } => {
                    let modified_weight_wu =
                        tx.weight().to_wu().saturating_add_signed(weight_tweak);
                    let modified_weight = Weight::from_wu(modified_weight_wu);
                    inner.mempool.insert(
                        tx.clone(),
                        fee_delta.to_sat(),
                        conflicts_with.into(),
                        modified_weight,
                    )?;
                    inner.unfiltered_mempool.txs.insert(txid);
                    tracing::trace!(%txid, "added tx to mempool");
                    Ok(true)
                }
                cusf_enforcer::TxAcceptAction::Reject => {
                    tracing::trace!(%txid, "rejecting tx");
                    // remove all descendants
                    let rejected_mempool_txs =
                        inner.mempool.remove_with_descendants(&txid)?;
                    let mut rejected_txs = HashSet::new();
                    for rejected_tx in std::iter::once(txid).chain(
                        rejected_mempool_txs.into_iter().map(|(txid, _)| txid),
                    ) {
                        rejected_txs.extend(
                            inner
                                .abandoned_pool
                                .remove_descendant_txs(&rejected_tx)
                                .into_iter()
                                .map(|(txid, _)| txid),
                        );
                        rejected_txs.insert(rejected_tx);
                    }
                    for rejected_tx in rejected_txs {
                        sync_state.rejected_txs.insert(rejected_tx);
                        sync_state
                            .request_queue
                            .push_front(RequestItem::RejectTx(rejected_tx));
                    }
                    inner.unfiltered_mempool.txs.insert(txid);
                    Ok(true)
                }
            }
        }
    }
}

async fn try_apply_seq_message<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    seq_msg: &SequenceMessage,
) -> Result<ApplySyncActionResult, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        }) => {
            if inner.mempool.chain.tip != *block_hash {
                return Ok(ApplySyncActionResult::Pending);
            }
            let Some(block) =
                inner.mempool.chain.blocks.get(block_hash).cloned()
            else {
                return Ok(ApplySyncActionResult::Pending);
            };
            let applied = handle_disconnected_block(
                inner,
                sync_state.blocks_needed,
                sync_state.request_queue,
                &block,
            )
            .await?;
            Ok(ApplySyncActionResult::from(applied))
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Added,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            let added = try_add_tx_from_caches(inner, sync_state, *txid)?;
            Ok(ApplySyncActionResult::from(added))
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Removed,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            if inner.unfiltered_mempool.txs.remove(txid) {
                inner.mempool.remove(txid)?;
                inner.abandoned_pool.remove(txid);
                Ok(ApplySyncActionResult::from(true))
            } else {
                Err(SyncTaskError::UnfilteredMempoolMissingTx(*txid))
            }
        }
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Connected,
            ..
        }) => {
            let Some(block) = inner.mempool.chain.blocks.get(block_hash) else {
                tracing::debug!(
                    %block_hash,
                    "waiting for block to connect"
                );
                return Ok(ApplySyncActionResult::Pending);
            };
            let () = connect_block(inner, sync_state, &block.clone()).await?;
            Ok(ApplySyncActionResult::from(true))
        }
    }
}

// returns `true` if an item was applied successfully
async fn try_apply_next_sync_action<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
) -> Result<bool, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let Some(next_sync_action) = sync_state.action_queue.front() else {
        tracing::trace!("no sync actions to apply in queue");
        return Ok(false);
    };
    let res = match next_sync_action.action {
        SyncAction::InsertTx(txid) => {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
            };
            try_add_tx_from_caches(inner, sync_state, txid)?.into()
        }
        SyncAction::SequenceMessage(seq_msg) => {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
            };
            try_apply_seq_message(inner, sync_state, &seq_msg).await?
        }
    };
    match res {
        ApplySyncActionResult::Success {
            push_txs_action_queue_front,
        } => {
            sync_state.action_queue.pop_front();
            for tx in push_txs_action_queue_front.into_iter().rev() {
                sync_state.action_queue.push_front(SyncAction::InsertTx(tx));
            }
            Ok(true)
        }
        ApplySyncActionResult::Pending => Ok(false),
    }
}

async fn handle_resp<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp: BatchedResponseItem,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match resp {
        BatchedResponseItem::BatchTx(txs) => {
            let mut input_txs_needed = LinkedHashSet::new();
            for (tx, in_mempool) in txs {
                if in_mempool {
                    for input_txid in
                        tx.input.iter().map(|input| input.previous_output.txid)
                    {
                        if !sync_state.tx_cache.contains_key(&input_txid) {
                            input_txs_needed.replace(input_txid);
                        }
                    }
                }
                let () = handle_resp_tx(sync_state, tx);
            }
            sync_state
                .txs_needed
                .extend(input_txs_needed.iter().copied());
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::Single(ResponseItem::Block(block)) => {
            tracing::debug!(%block.hash, "Handling block response");
            let () = handle_resp_block(inner, sync_state, *block).await?;
        }
        BatchedResponseItem::Single(ResponseItem::Tx(tx, in_mempool)) => {
            let mut input_txs_needed = LinkedHashSet::new();
            if in_mempool {
                for input_txid in
                    tx.input.iter().map(|input| input.previous_output.txid)
                {
                    if !sync_state.tx_cache.contains_key(&input_txid) {
                        input_txs_needed.replace(input_txid);
                    }
                }
            }
            let () = handle_resp_tx(sync_state, *tx);
            sync_state
                .txs_needed
                .extend(input_txs_needed.iter().copied());
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::BatchRejectTx
        | BatchedResponseItem::Single(ResponseItem::RejectBlock)
        | BatchedResponseItem::Single(ResponseItem::RejectTx) => {}
    }
    while try_apply_next_sync_action(inner, sync_state).await? {}
    Ok(())
}

trait ContinueLoop<const EXIT_AFTER_SYNC: bool> {
    async fn continue_loop<Enforcer>(
        inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        sync_state: &SyncState,
    ) -> bool;
}

struct ContinueLoopImpl;

impl ContinueLoop<true> for ContinueLoopImpl {
    async fn continue_loop<Enforcer>(
        inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        sync_state: &SyncState,
    ) -> bool {
        let inner_read = inner.read().await;
        !(MempoolSyncingBorrowed {
            inner: &inner_read,
            sync_state,
        }
        .is_synced())
    }
}

impl ContinueLoop<false> for ContinueLoopImpl {
    async fn continue_loop<Enforcer>(
        _inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        _sync_state: &SyncState,
    ) -> bool {
        true
    }
}

/// If `EXIT_AFTER_SYNC` is `true`, then returns once syncing is complete.
/// Otherwise, continues until the shutdown signal is encountered.
async fn task_inner<
    const EXIT_AFTER_SYNC: bool,
    Enforcer,
    BorrowedEnforcer,
    ShutdownSignal,
>(
    inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
    sync_state: &mut SyncState,
    combined_stream: &mut CombinedStream<
        'static,
        'static,
        futures::future::Fuse<BoxFuture<'static, ()>>,
        ShutdownSignal,
    >,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
    ShutdownSignal: FusedFuture<Output = ()> + Send + Unpin,
    ContinueLoopImpl: ContinueLoop<EXIT_AFTER_SYNC>,
{
    while <ContinueLoopImpl as ContinueLoop<EXIT_AFTER_SYNC>>::continue_loop(
        inner, sync_state,
    )
    .await
    {
        let msg = combined_stream
            .next()
            .await
            .ok_or(SyncTaskError::CombinedStreamEnded)?;
        let msg_kind = match &msg {
            CombinedStreamItem::ZmqSeq(_) => "sequence",
            CombinedStreamItem::Response(_) => "response",
            CombinedStreamItem::ApplySyncActionTimeout => {
                "apply seq action timeout"
            }
            CombinedStreamItem::Shutdown => "shutdown",
        };

        let span = tracing::debug_span!(
            "handle_stream_msg",
            sequence_id = ulid::Ulid::new().to_string(), // ULIDs are clickable, short and sorts naturally by time
        );

        tracing::debug!(parent: &span, "Handling {msg_kind} stream message");

        match msg {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let seq_msg = seq_msg?;
                let mut fut = async || {
                    let mut inner_write = inner.write().await;
                    let () = handle_seq_message(
                        &mut inner_write,
                        sync_state,
                        seq_msg,
                    )?;
                    while try_apply_next_sync_action(
                        &mut inner_write,
                        sync_state,
                    )
                    .await?
                    {}
                    Ok::<_, SyncTaskError<_>>(())
                };
                let () = fut().instrument(span).await?;
                combined_stream.apply_sync_action_timeout =
                    apply_sync_action_timeout(&sync_state.action_queue).fuse();
            }
            CombinedStreamItem::Response(resp) => {
                let resp = resp.map_err(SyncTaskError::Request)?;
                {
                    let mut inner_write = inner.write().await;
                    let () = handle_resp(&mut inner_write, sync_state, resp)
                        .instrument(span)
                        .await?;
                }
                combined_stream.apply_sync_action_timeout =
                    apply_sync_action_timeout(&sync_state.action_queue).fuse();
            }
            CombinedStreamItem::ApplySyncActionTimeout => {
                let err = ApplySyncActionTimeoutError {
                    action: sync_state
                        .action_queue
                        .front()
                        .as_ref()
                        .map(|head| head.action),
                };
                return Err(err.into());
            }
            CombinedStreamItem::Shutdown => {
                tracing::info!(parent: &span, "shutdown signal received, aborting");
                return Err(SyncTaskError::Shutdown);
            }
        }
    }
    Ok(())
}

pub struct MempoolSynced<Enforcer, ShutdownSignal>
where
    ShutdownSignal: Future,
{
    // Uses a dummy value for the enforcer argument.
    // The caller must provide an owned enforcer when creating a `MempoolSync`
    // from this.
    inner: MempoolSyncing<()>,
    combined_stream: CombinedStream<
        'static,
        'static,
        futures::future::Fuse<BoxFuture<'static, ()>>,
        futures::future::Shared<ShutdownSignal>,
    >,
    _marker: PhantomData<Enforcer>,
}

pub async fn init_sync_mempool<
    Enforcer,
    BorrowedEnforcer,
    RpcClient,
    ShutdownSignal,
>(
    mut enforcer: Enforcer,
    network: bitcoin::Network,
    rpc_client: RpcClient,
    zmq_addr_sequence: &str,
    // Would it be better to return a Some/None, indicating sync stoppage?
    shutdown_signal: ShutdownSignal,
) -> Result<
    MempoolSynced<BorrowedEnforcer, ShutdownSignal>,
    SyncTaskError<BorrowedEnforcer>,
>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Send + Sync + 'static,
    ShutdownSignal: FusedFuture<Output = ()> + Send + Unpin,
{
    let shutdown_signal = shutdown_signal.shared();
    let (best_block_hash, sequence_stream) = cusf_enforcer::initial_sync(
        enforcer.borrow_mut(),
        &rpc_client,
        zmq_addr_sequence,
        shutdown_signal.clone(),
    )
    .await?;
    let RawMempoolWithSequence {
        txids,
        mempool_sequence,
    } = rpc_client
        .get_raw_mempool(BoolWitness::<false>, BoolWitness::<true>)
        .await?;
    let inner = MempoolSyncInner {
        abandoned_pool: AbandonedPool::default(),
        enforcer,
        mempool: Mempool::new(network, best_block_hash),
        unfiltered_mempool: UnfilteredMempool {
            tip: best_block_hash,
            txs: HashSet::new(),
        },
    };
    let inner = TokioRwLock::new(inner);
    let mut sync_state = {
        let request_queue = RequestQueue::default();
        request_queue.push_back(RequestItem::Block(best_block_hash));
        for txid in &txids {
            request_queue.push_back(RequestItem::Tx(*txid, true));
        }
        let action_queue = SyncActionQueue::from_iter(
            txids.iter().cloned().map(SyncAction::InsertTx),
        );
        SyncState {
            action_queue,
            blocks_needed: LinkedHashSet::from_iter([best_block_hash]),
            first_mempool_sequence: Some(mempool_sequence + 1),
            rejected_txs: HashSet::new(),
            request_queue,
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::from_iter(txids),
            unavailable_txs: HashSet::new(),
        }
    };

    let response_stream = sync_state
        .request_queue
        .clone()
        .then({
            let rpc_client = Arc::new(rpc_client);
            move |request| {
                let rpc_client = rpc_client.clone();
                async move {
                    batched_request::<RpcClient>(&rpc_client, request).await
                }
            }
        })
        .boxed();

    let mut combined_stream = CombinedStream::new(
        sequence_stream,
        response_stream,
        apply_sync_action_timeout(&sync_state.action_queue).fuse(),
        shutdown_signal,
    );
    let () = task_inner::<true, _, _, _>(
        &inner,
        &mut sync_state,
        &mut combined_stream,
    )
    .await?;

    let res = {
        let inner = {
            let MempoolSyncInner {
                abandoned_pool,
                enforcer: _,
                mempool,
                unfiltered_mempool,
            } = inner.into_inner();
            MempoolSyncing {
                inner: MempoolSyncInner {
                    abandoned_pool,
                    enforcer: (),
                    mempool,
                    unfiltered_mempool,
                },
                sync_state,
            }
        };
        MempoolSynced {
            inner,
            combined_stream,
            _marker: PhantomData,
        }
    };
    Ok(res)
}

pub struct MempoolSync<Enforcer> {
    inner: std::sync::Weak<TokioRwLock<MempoolSyncInner<Enforcer>>>,
    _task: tokio_util::task::AbortOnDropHandle<()>,
}

impl<Enforcer> MempoolSync<Enforcer>
where
    Enforcer: CusfEnforcer + Send + Sync + 'static,
{
    pub fn new<ShutdownSignal, ErrHandler, ErrHandlerFut>(
        enforcer: Enforcer,
        mempool_synced: MempoolSynced<Enforcer, ShutdownSignal>,
        err_handler: ErrHandler,
    ) -> Self
    where
        ShutdownSignal: Future<Output = ()> + Send + Sync + 'static,
        ErrHandler:
            FnOnce(SyncTaskError<Enforcer>) -> ErrHandlerFut + Send + 'static,
        ErrHandlerFut: Future<Output = ()> + Send,
    {
        let MempoolSynced {
            inner,
            mut combined_stream,
            _marker,
        } = mempool_synced;
        let (inner, mut sync_state) = {
            let MempoolSyncing {
                inner:
                    MempoolSyncInner {
                        abandoned_pool,
                        enforcer: (),
                        mempool,
                        unfiltered_mempool,
                    },
                sync_state,
            } = inner;
            let inner = MempoolSyncInner {
                abandoned_pool,
                enforcer,
                mempool,
                unfiltered_mempool,
            };
            (inner, sync_state)
        };
        let inner = Arc::new(TokioRwLock::new(inner));
        let inner_weak = Arc::downgrade(&inner);
        let task = tokio::task::spawn(async move {
            match task_inner::<false, _, _, _>(
                &inner,
                &mut sync_state,
                &mut combined_stream,
            )
            .await
            {
                Ok(_) => {}
                Err(err) => err_handler(err).await,
            }
        });
        Self {
            inner: inner_weak,
            _task: tokio_util::task::AbortOnDropHandle::new(task),
        }
    }

    /// Apply a function over the mempool and enforcer.
    /// Returns `None` if the mempool is unavailable due to an error.
    pub async fn with<F, Output>(&self, f: F) -> Option<Output>
    where
        F: for<'a> FnOnce(&'a Mempool, &'a Enforcer) -> BoxFuture<'a, Output>,
    {
        let inner = self.inner.upgrade()?;
        let inner_read = inner.read().await;
        let res = f(&inner_read.mempool, &inner_read.enforcer).await;
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, BlockHash, Network, OutPoint, ScriptBuf, Sequence, Transaction,
        TxIn, TxOut, Txid, Witness, absolute::LockTime, hashes::Hash as _,
        transaction::Version,
    };
    use hashlink::LinkedHashSet;

    use super::*;
    use crate::{cusf_enforcer::DefaultEnforcer, mempool::Mempool};

    fn make_tx(inputs: &[OutPoint], output_values: &[u64]) -> Transaction {
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
            output: output_values
                .iter()
                .map(|value| TxOut {
                    value: Amount::from_sat(*value),
                    script_pubkey: ScriptBuf::new(),
                })
                .collect(),
        }
    }

    /// Regression test for the initial-sync crash reported in
    /// LayerTwo-Labs/bip300301_enforcer#406.
    ///
    /// During initial sync the RPC mempool snapshot and the live ZMQ
    /// `sequence` stream are reconciled together, so the same txid can be
    /// queued for insertion twice (e.g. once as the snapshot `InsertTx` and
    /// once as a buffered `Added` sequence message). The second application
    /// must be a no-op success rather than aborting the entire sync task with
    /// `MempoolInsertError::TxAlreadyExists`.
    #[test]
    fn add_tx_from_caches_is_idempotent() {
        let genesis = BlockHash::all_zeros();
        let mut inner = MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(Network::Regtest, genesis),
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        };

        // A parent tx supplying a spendable output, available only via the tx
        // cache (it is not itself a mempool tx).
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        // The tx under test, spending the parent's output.
        let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let txid = tx.compute_txid();

        let mut tx_cache = HashMap::new();
        tx_cache.insert(parent_txid, parent);
        tx_cache.insert(txid, tx);

        let mut blocks_needed = LinkedHashSet::new();
        let mut rejected_txs = HashSet::new();
        let request_queue = RequestQueue::default();
        let mut txs_needed = LinkedHashSet::new();
        let mut unavailable_txs = HashSet::new();

        // First insertion adds the tx to the mempool.
        {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut blocks_needed,
                rejected_txs: &mut rejected_txs,
                request_queue: &request_queue,
                tx_cache: &mut tx_cache,
                txs_needed: &mut txs_needed,
                unavailable_txs: &mut unavailable_txs,
            };
            let added = try_add_tx_from_caches::<_, DefaultEnforcer>(
                &mut inner, sync_state, txid,
            )
            .expect("first add should succeed");
            assert!(added, "tx should be added to the mempool");
        }
        assert!(
            inner.mempool.txs.0.contains_key(&txid),
            "tx should be present in the mempool after the first add"
        );

        // Re-applying the same txid must be an idempotent no-op success, not a
        // fatal `TxAlreadyExists`.
        {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut blocks_needed,
                rejected_txs: &mut rejected_txs,
                request_queue: &request_queue,
                tx_cache: &mut tx_cache,
                txs_needed: &mut txs_needed,
                unavailable_txs: &mut unavailable_txs,
            };
            let added = try_add_tx_from_caches::<_, DefaultEnforcer>(
                &mut inner, sync_state, txid,
            )
            .expect(
                "re-adding an already-present tx must not error \
                 (idempotent reconciliation)",
            );
            assert!(added, "re-add should report success");
        }
    }
}
