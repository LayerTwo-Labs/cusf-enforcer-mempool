use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    future::Future,
    sync::Arc,
};

use bitcoin::{
    consensus::Decodable, hashes::Hash as _, Amount, BlockHash, OutPoint,
    Transaction, TxIn, Txid,
};
use educe::Educe;
use futures::{future::BoxFuture, stream, StreamExt as _};
use hashlink::LinkedHashSet;
use imbl::HashSet;
use thiserror::Error;
use tokio::{spawn, sync::RwLock, task::JoinHandle};
use tracing::instrument;

use super::{
    super::{Conflicts, Mempool},
    batched_request, BatchedResponseItem, CombinedStreamItem, RequestError,
    RequestQueue, ResponseItem,
};
use crate::{
    cusf_enforcer::{
        self, ConnectBlockAction, CusfEnforcer, DisconnectBlockAction,
    },
    mempool::{
        sync::RequestItem, MempoolInsertError, MempoolRemoveError,
        MempoolUpdateError,
    },
    zmq::{
        BlockHashEvent, BlockHashMessage, SequenceMessage, SequenceStream,
        SequenceStreamError, TxHashEvent, TxHashMessage,
    },
};

#[derive(Debug)]
enum SyncAction {
    /// Insert tx
    InsertTx(Txid),
    /// Apply ZMQ sequence message
    SequenceMessage(SequenceMessage),
}

#[derive(Debug)]
pub struct SyncState {
    /// Txs rejected by the CUSF enforcer
    rejected_txs: HashSet<Txid>,
    request_queue: RequestQueue,
    /// Queued actions to apply.
    action_queue: VecDeque<SyncAction>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
}

/// Sync state, mutably borrowed while applying an action from the queue
struct SyncStateBorrowedMut<'a> {
    /// Txs rejected by the CUSF enforcer
    rejected_txs: &'a mut HashSet<Txid>,
    request_queue: &'a RequestQueue,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: &'a mut HashMap<Txid, Transaction>,
}

#[derive(Educe)]
#[educe(Debug(bound(cusf_enforcer::Error<Enforcer>: std::fmt::Debug)))]
#[derive(Error)]
pub enum SyncTaskError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error(transparent)]
    CusfEnforcer(#[from] cusf_enforcer::Error<Enforcer>),
    #[error("Failed to decode block: `{block_hash}`")]
    DecodeBlock {
        block_hash: BlockHash,
        source: bitcoin::consensus::encode::Error,
    },
    #[error("Failed to decode transaction: `{txid}`")]
    DecodeTransaction {
        txid: Txid,
        source: bitcoin::consensus::encode::Error,
    },
    #[error("Fee overflow")]
    FeeOverflow,
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error(transparent)]
    MempoolUpdate(#[from] MempoolUpdateError),
    #[error("Request error")]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sync was stopped")]
    Shutdown,
    #[error("Value in overflow")]
    ValueInOverflow,
    #[error("Value out overflow")]
    ValueOutOverflow,
}

struct MempoolSyncInner<Enforcer> {
    enforcer: Enforcer,
    mempool: Mempool,
}

async fn handle_seq_message(
    sync_state: &mut SyncState,
    seq_msg: SequenceMessage,
) {
    match seq_msg {
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Connected,
            ..
        }) => {
            // FIXME: handle case in which block exists
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            sync_state
                .request_queue
                .push_back(RequestItem::Block(block_hash));
        }
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        }) => {
            tracing::debug!(block_hash = %block_hash, "Adding disconnected block to req queue");

            // This will cause us to fetch the block, and then handle the actual
            // disconnect. Invalidated blocks return just fine from `getblock`,
            // with a -1 confirmations count
            sync_state
                .request_queue
                .push_back(RequestItem::Block(block_hash));
        }

        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Added,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            // FIXME: remove
            tracing::debug!("Added {txid} to req queue");
            sync_state
                .request_queue
                .push_back(RequestItem::Tx(txid, true));
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Removed,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            tracing::debug!("Remove tx {txid} from req queue");
            sync_state
                .request_queue
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    sync_state
        .action_queue
        .push_back(SyncAction::SequenceMessage(seq_msg));
}

fn handle_resp_tx(sync_state: &mut SyncState, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.tx_cache.insert(txid, tx);
}

async fn connect_block<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let mut removed_txids = Vec::new();
    for tx_info in &block.tx {
        let txid = tx_info.txid;
        if let Some((tx, _)) = inner.mempool.remove(&txid)? {
            removed_txids.push(tx.compute_txid());
        };
        sync_state
            .request_queue
            .remove(&RequestItem::Tx(txid, true));
    }
    inner.mempool.chain.tip = block.hash;
    let block_decoded =
        block.try_into().map_err(|err| SyncTaskError::DecodeBlock {
            block_hash: block.hash,
            source: err,
        })?;

    tracing::trace!(
        block_hash = %block.hash,
        block_height = block.height,
        "processed mempool, forwarding to enforcer block connection logic");
    match inner
        .enforcer
        .connect_block(&block_decoded)
        .await
        .map_err(cusf_enforcer::Error::ConnectBlock)?
    {
        ConnectBlockAction::Accept { remove_mempool_txs } => {
            let _removed_txs = inner
                .mempool
                .try_filter(true, |tx, _| {
                    Ok::<_, Infallible>(
                        !remove_mempool_txs.contains(&tx.compute_txid()),
                    )
                })
                .map_err(|err| {
                    let either::Either::Left(err) = err;
                    SyncTaskError::MempoolRemove(err)
                })?;
        }
        ConnectBlockAction::Reject => {
            // FIXME: reject block
        }
    };
    Ok(())
}

// Returns input txs from cache, or requests that need to be queued.
// Returns `None` if an input tx was rejected.
fn try_get_input_txs_from_cache<'a>(
    mempool: &'a Mempool,
    rejected_txs: &'a HashSet<Txid>,
    tx_cache: &'a HashMap<Txid, Transaction>,
    vin: &[TxIn],
) -> Result<Option<HashMap<Txid, &'a Transaction>>, LinkedHashSet<Txid>> {
    let mut input_txs_needed = LinkedHashSet::new();
    let mut input_txs = HashMap::<Txid, &Transaction>::new();
    for input in vin {
        let OutPoint {
            txid: input_txid,
            vout: _,
        } = input.previous_output;
        let input_tx = if rejected_txs.contains(&input_txid) {
            return Ok(None);
        } else if let Some(input_tx) = tx_cache.get(&input_txid) {
            input_tx
        } else if let Some((input_tx, _)) = mempool.txs.0.get(&input_txid) {
            input_tx
        } else {
            input_txs_needed.replace(input_txid);
            continue;
        };
        input_txs.insert(input_txid, input_tx);
    }
    if input_txs_needed.is_empty() {
        Ok(Some(input_txs))
    } else {
        Err(input_txs_needed)
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

/// Returns a vec of txs that must be pushed to the front of the action queue
/// to be applied
#[instrument(skip_all, fields(block_hash = %block.hash))]
async fn handle_disconnected_block<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<Vec<Txid>, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let DisconnectBlockAction { remove_mempool_txs } = inner
        .enforcer
        .disconnect_block(block.hash)
        .await
        .map_err(cusf_enforcer::Error::DisconnectBlock)?;
    let _removed_txs = inner
        .mempool
        .try_filter(true, |tx, _| {
            Ok::<_, Infallible>(
                !remove_mempool_txs.contains(&tx.compute_txid()),
            )
        })
        .map_err(|err| {
            let either::Either::Left(err) = err;
            SyncTaskError::MempoolRemove(err)
        })?;
    let block_parent =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    // Mempool is now valid for the block parent
    inner.mempool.chain.tip = block_parent;

    let mut txs_to_apply_later = Vec::new();
    let mut input_txs_needed = LinkedHashSet::new();
    let mut inserted_txs = 0;
    // Insert any block transactions back into the mempool
    for tx_info in &block.tx {
        let decoded =
            Transaction::consensus_decode(&mut tx_info.hex.as_slice())
                .map_err(|err| SyncTaskError::DecodeTransaction {
                    txid: tx_info.txid,
                    source: err,
                })?;

        // Skip coinbase TXs
        if decoded.is_coinbase() {
            continue;
        }

        let input_txs = match try_get_input_txs_from_cache(
            &inner.mempool,
            sync_state.rejected_txs,
            sync_state.tx_cache,
            &decoded.input,
        ) {
            Err(input_txs_needed_for_tx) => {
                input_txs_needed.extend(input_txs_needed_for_tx);
                sync_state.tx_cache.insert(tx_info.txid, decoded);
                txs_to_apply_later.push(tx_info.txid);
                continue;
            }
            Ok(None) => {
                // Should be impossible during a disconnect, but handled anyway
                let txid = tx_info.txid;
                // Reject tx
                tracing::trace!("rejecting {txid}: rejected ancestor");
                sync_state.rejected_txs.insert(txid);
                sync_state
                    .request_queue
                    .push_front(RequestItem::RejectTx(txid));
                continue;
            }
            Ok(Some(input_txs)) => input_txs,
        };
        // Txs must be applied in order
        if !txs_to_apply_later.is_empty() {
            sync_state.tx_cache.insert(tx_info.txid, decoded);
            txs_to_apply_later.push(tx_info.txid);
            continue;
        }
        let fee_delta = fee_delta(&decoded, &input_txs)?;
        match inner
            .enforcer
            .accept_tx(&decoded, &input_txs)
            .map_err(cusf_enforcer::Error::AcceptTx)?
        {
            cusf_enforcer::TxAcceptAction::Accept { conflicts_with } => {
                inner.mempool.insert(
                    decoded,
                    fee_delta.to_sat(),
                    conflicts_with.into(),
                )?;
                tracing::trace!("added {} to mempool", tx_info.txid);
                inserted_txs += 1;
            }
            cusf_enforcer::TxAcceptAction::Reject => {
                let txid = tx_info.txid;
                tracing::trace!("rejecting {txid}");
                sync_state.rejected_txs.insert(txid);
                sync_state
                    .request_queue
                    .push_front(RequestItem::RejectTx(txid));
            }
        }
    }

    if inserted_txs > 0 {
        tracing::debug!("inserted {inserted_txs} txs back into the mempool");
    }
    for tx in input_txs_needed.into_iter().rev() {
        sync_state
            .request_queue
            .push_front(RequestItem::Tx(tx, false));
    }
    tracing::debug!("disconnected block");
    Ok(txs_to_apply_later)
}

async fn handle_resp_block<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp_block: bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    inner
        .mempool
        .chain
        .blocks
        .insert(resp_block.hash, resp_block.clone());
    let Some(SyncAction::SequenceMessage(SequenceMessage::BlockHash(
        block_hash_msg,
    ))) = sync_state.action_queue.front()
    else {
        return Ok(());
    };
    match block_hash_msg.event {
        BlockHashEvent::Connected => {
            if block_hash_msg.block_hash != resp_block.hash {
                return Ok(());
            }
            {
                let sync_state = SyncStateBorrowedMut {
                    rejected_txs: &mut sync_state.rejected_txs,
                    request_queue: &sync_state.request_queue,
                    tx_cache: &mut sync_state.tx_cache,
                };
                let () = connect_block(inner, sync_state, &resp_block).await?;
            }
            sync_state.action_queue.pop_front();
        }
        BlockHashEvent::Disconnected => {
            if !(block_hash_msg.block_hash == resp_block.hash
                && inner.mempool.chain.tip == resp_block.hash)
            {
                return Ok(());
            };

            let txs_to_apply_later = {
                let sync_state = SyncStateBorrowedMut {
                    rejected_txs: &mut sync_state.rejected_txs,
                    request_queue: &sync_state.request_queue,
                    tx_cache: &mut sync_state.tx_cache,
                };
                handle_disconnected_block(inner, sync_state, &resp_block)
                    .await?
            };
            sync_state.action_queue.pop_front();
            for tx in txs_to_apply_later.into_iter().rev() {
                sync_state.action_queue.push_front(SyncAction::InsertTx(tx));
            }
        }
    }
    Ok(())
}

// returns `true` if the tx was added or rejected successfully
async fn try_add_tx_from_cache<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    txid: &Txid,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return Ok(false);
    };

    let input_txs = match try_get_input_txs_from_cache(
        &inner.mempool,
        sync_state.rejected_txs,
        sync_state.tx_cache,
        &tx.input,
    ) {
        Err(input_txs_needed) => {
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
            return Ok(false);
        }
        Ok(None) => {
            // Reject tx
            tracing::trace!("rejecting {txid}: rejected ancestor");
            sync_state.rejected_txs.insert(*txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(*txid));
            return Ok(true);
        }
        Ok(Some(input_txs)) => input_txs,
    };

    let fee_delta = fee_delta(tx, &input_txs)?;
    match inner
        .enforcer
        .accept_tx(tx, &input_txs)
        .map_err(cusf_enforcer::Error::AcceptTx)?
    {
        cusf_enforcer::TxAcceptAction::Accept { conflicts_with } => {
            inner.mempool.insert(
                tx.clone(),
                fee_delta.to_sat(),
                conflicts_with.into(),
            )?;
            tracing::trace!("added {txid} to mempool");
        }
        cusf_enforcer::TxAcceptAction::Reject => {
            tracing::trace!("rejecting {txid}");
            sync_state.rejected_txs.insert(*txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(*txid));
        }
    }
    let mempool_txs = inner.mempool.txs.0.len();
    tracing::debug!(%mempool_txs, "Syncing...");
    Ok(true)
}

enum ApplySyncActionResult {
    /// Sync action applied successfully
    Success {
        /// Txs that should be pushed at the front of the action queue to be
        /// applied
        push_txs_action_queue_front: Vec<Txid>,
    },
    /// Sync action could not be applied
    Pending,
}

impl From<bool> for ApplySyncActionResult {
    fn from(success: bool) -> Self {
        if success {
            Self::Success {
                push_txs_action_queue_front: Vec::new(),
            }
        } else {
            Self::Pending
        }
    }
}

// returns `true` if a sequence message was applied successfully
async fn try_apply_seq_message<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    seq_msg: &SequenceMessage,
) -> Result<ApplySyncActionResult, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        }) => {
            if inner.mempool.chain.tip != *block_hash {
                tracing::debug!(block_hash = %block_hash, "Block hash mismatch, skipping");
                return Ok(ApplySyncActionResult::Pending);
            };
            let Some(block) =
                inner.mempool.chain.blocks.get(block_hash).cloned()
            else {
                tracing::debug!(block_hash = %block_hash, "Block not found, skipping");
                return Ok(ApplySyncActionResult::Pending);
            };

            let push_txs_action_queue_front =
                handle_disconnected_block(inner, sync_state, &block).await?;
            Ok(ApplySyncActionResult::Success {
                push_txs_action_queue_front,
            })
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Added,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            let txid = *txid;
            try_add_tx_from_cache(inner, sync_state, &txid)
                .await
                .map(ApplySyncActionResult::from)
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Removed,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            // FIXME: review -- looks sus
            let res = inner.mempool.remove(txid)?.is_some();
            Ok(ApplySyncActionResult::from(res))
        }
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Connected,
            ..
        }) => {
            let Some(block) =
                inner.mempool.chain.blocks.get(block_hash).cloned()
            else {
                return Ok(ApplySyncActionResult::Pending);
            };
            let parent =
                block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
            assert_eq!(inner.mempool.chain.tip, parent);
            let () = connect_block(inner, sync_state, &block).await?;
            Ok(ApplySyncActionResult::from(true))
        }
    }
}

// returns `true` if an action was applied successfully
async fn try_apply_next_sync_action<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let res = match sync_state.action_queue.front() {
        Some(SyncAction::InsertTx(txid)) => {
            let txid = *txid;
            let sync_state = SyncStateBorrowedMut {
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
            };
            try_add_tx_from_cache(inner, sync_state, &txid)
                .await?
                .into()
        }
        Some(SyncAction::SequenceMessage(seq_msg)) => {
            let sync_state = SyncStateBorrowedMut {
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
            };
            try_apply_seq_message(inner, sync_state, seq_msg).await?
        }
        None => false.into(),
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

async fn handle_resp<Enforcer>(
    inner: &RwLock<MempoolSyncInner<Enforcer>>,
    sync_state: &mut SyncState,
    resp: BatchedResponseItem,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let mut inner_write = inner.write().await;
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
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::Single(ResponseItem::Block(block)) => {
            let () =
                handle_resp_block(&mut inner_write, sync_state, *block).await?;
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
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::BatchRejectTx
        | BatchedResponseItem::Single(ResponseItem::RejectTx) => {}
    }
    while try_apply_next_sync_action(&mut inner_write, sync_state).await? {}
    Ok(())
}

async fn task<Enforcer, RpcClient>(
    inner: Arc<RwLock<MempoolSyncInner<Enforcer>>>,
    tx_cache: HashMap<Txid, Transaction>,
    rpc_client: RpcClient,
    sequence_stream: SequenceStream<'static>,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Sync,
{
    // Filter mempool with enforcer
    let rejected_txs: LinkedHashSet<Txid> = {
        let mut inner_write = inner.write().await;
        let MempoolSyncInner {
            ref mut enforcer,
            ref mut mempool,
        } = *inner_write;
        let mut conflicts = Conflicts::default();
        let rejected_txs = mempool
            .try_filter(true, |tx, mempool_inputs| {
                let mut tx_inputs = mempool_inputs.clone();
                for tx_in in &tx.input {
                    let input_txid = tx_in.previous_output.txid;
                    if tx_inputs.contains_key(&input_txid) {
                        continue;
                    }
                    let input_tx = &tx_cache[&input_txid];
                    tx_inputs.insert(input_txid, input_tx);
                }
                match enforcer.accept_tx(tx, &tx_inputs)? {
                    cusf_enforcer::TxAcceptAction::Accept {
                        conflicts_with,
                    } => {
                        let txid = tx.compute_txid();
                        for conflict_txid in conflicts_with {
                            conflicts.insert(txid, conflict_txid);
                        }
                        Ok(true)
                    }
                    cusf_enforcer::TxAcceptAction::Reject => Ok(false),
                }
            })
            .map_err(|err| match err {
                either::Either::Left(mempool_remove_err) => {
                    SyncTaskError::MempoolRemove(mempool_remove_err)
                }
                either::Either::Right(enforcer_err) => {
                    let err = cusf_enforcer::Error::AcceptTx(enforcer_err);
                    SyncTaskError::CusfEnforcer(err)
                }
            })?
            .keys()
            .copied()
            .collect();
        let () = mempool.add_conflicts(conflicts)?;
        drop(inner_write);
        rejected_txs
    };
    let request_queue = RequestQueue::default();
    let rejected_txs: HashSet<Txid> = rejected_txs
        .into_iter()
        .inspect(|rejected_txid| {
            request_queue.push_back(RequestItem::RejectTx(*rejected_txid));
        })
        .collect();
    let mut sync_state = SyncState {
        rejected_txs,
        request_queue,
        action_queue: VecDeque::new(),
        tx_cache,
    };
    let response_stream = sync_state
        .request_queue
        .clone()
        .then(|request| batched_request(&rpc_client, request))
        .boxed();
    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        response_stream.map(CombinedStreamItem::Response),
    );
    loop {
        let msg = combined_stream
            .next()
            .await
            .ok_or(SyncTaskError::CombinedStreamEnded)?;

        tracing::debug!(
            "Received stream message: `{}`",
            match msg {
                CombinedStreamItem::ZmqSeq(_) => "sequence",
                CombinedStreamItem::Response(_) => "response",
                CombinedStreamItem::Shutdown => "shutdown",
            }
        );

        match msg {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () = handle_seq_message(&mut sync_state, seq_msg?).await;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&inner, &mut sync_state, resp?).await?;
            }
            CombinedStreamItem::Shutdown => {
                tracing::info!("shutdown signal received, aborting");
                // This isn't really an error though...
                return Err(SyncTaskError::Shutdown);
            }
        };
    }
}

pub struct MempoolSync<Enforcer> {
    inner: std::sync::Weak<RwLock<MempoolSyncInner<Enforcer>>>,
    task: JoinHandle<()>,
}

impl<Enforcer> MempoolSync<Enforcer>
where
    Enforcer: CusfEnforcer + Send + Sync + 'static,
{
    pub fn new<RpcClient, ErrHandler, ErrHandlerFut>(
        enforcer: Enforcer,
        mempool: Mempool,
        tx_cache: HashMap<Txid, Transaction>,
        rpc_client: RpcClient,
        sequence_stream: SequenceStream<'static>,
        err_handler: ErrHandler,
    ) -> Self
    where
        RpcClient:
            bitcoin_jsonrpsee::client::MainClient + Send + Sync + 'static,
        ErrHandler:
            FnOnce(SyncTaskError<Enforcer>) -> ErrHandlerFut + Send + 'static,
        ErrHandlerFut: Future<Output = ()> + Send,
    {
        let inner = MempoolSyncInner { enforcer, mempool };
        let inner = Arc::new(RwLock::new(inner));
        let inner_weak = Arc::downgrade(&inner);
        let task = spawn(async {
            match task(inner, tx_cache, rpc_client, sequence_stream).await {
                Ok(_) => {}
                Err(err) => err_handler(err).await,
            }
        });
        Self {
            inner: inner_weak,
            task,
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

impl<Enforcer> Drop for MempoolSync<Enforcer> {
    fn drop(&mut self) {
        self.task.abort()
    }
}
