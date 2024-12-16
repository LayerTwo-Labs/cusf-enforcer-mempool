use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    future::Future,
    sync::Arc,
};

use bitcoin::{
    hashes::Hash as _, Amount, BlockHash, OutPoint, Transaction, Txid,
};
use educe::Educe;
use futures::{stream, StreamExt as _};
use hashlink::LinkedHashSet;
use imbl::HashSet;
use thiserror::Error;
use tokio::{spawn, sync::RwLock, task::JoinHandle};

use super::{
    super::Mempool, batched_request, BatchedResponseItem, CombinedStreamItem,
    RequestError, RequestQueue, ResponseItem,
};
use crate::{
    cusf_enforcer::{self, ConnectBlockAction, CusfEnforcer},
    mempool::{sync::RequestItem, MempoolInsertError, MempoolRemoveError},
    zmq::{
        BlockHashEvent, BlockHashMessage, SequenceMessage, SequenceStream,
        SequenceStreamError, TxHashEvent, TxHashMessage,
    },
};

#[derive(Debug)]
pub struct SyncState {
    /// Txs rejected by the CUSF enforcer
    rejected_txs: HashSet<Txid>,
    request_queue: RequestQueue,
    seq_message_queue: VecDeque<SequenceMessage>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
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
    #[error("Fee overflow")]
    FeeOverflow,
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error("Request error")]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
}

struct MempoolSyncInner<Enforcer> {
    enforcer: Enforcer,
    mempool: Mempool,
}

async fn handle_seq_message<Enforcer>(
    inner: &RwLock<MempoolSyncInner<Enforcer>>,
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
            // FIXME: handle case in which block exists
            // FIXME: remove
            tracing::debug!("Adding block {block_hash} to req queue");
            if !inner
                .read()
                .await
                .mempool
                .chain
                .blocks
                .contains_key(&block_hash)
            {
                sync_state
                    .request_queue
                    .push_back(RequestItem::Block(block_hash));
            }
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
    sync_state.seq_message_queue.push_back(seq_msg);
}

fn handle_resp_tx(sync_state: &mut SyncState, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.tx_cache.insert(txid, tx);
}

async fn handle_resp_block<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp_block: bip300301::client::Block<true>,
) -> Result<(), SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let resp_block_parent = resp_block
        .previousblockhash
        .unwrap_or_else(BlockHash::all_zeros);
    inner
        .mempool
        .chain
        .blocks
        .insert(resp_block.hash, resp_block.clone());
    let Some(SequenceMessage::BlockHash(block_hash_msg)) =
        sync_state.seq_message_queue.front()
    else {
        return Ok(());
    };
    match block_hash_msg.event {
        BlockHashEvent::Connected => {
            if block_hash_msg.block_hash != resp_block.hash {
                return Ok(());
            }
            for tx_info in &resp_block.tx {
                let txid = tx_info.txid;
                let _removed: Option<_> = inner.mempool.remove(&txid)?;
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(txid, true));
            }
            inner.mempool.chain.tip = resp_block.hash;
            let resp_block_decoded =
                (&resp_block).try_into().map_err(|err| {
                    SyncTaskError::DecodeBlock {
                        block_hash: resp_block.hash,
                        source: err,
                    }
                })?;
            match inner
                .enforcer
                .connect_block(&resp_block_decoded)
                .map_err(cusf_enforcer::Error::ConnectBlock)?
            {
                ConnectBlockAction::Accept { remove_mempool_txs } => {
                    let _removed_txs = inner
                        .mempool
                        .try_filter(true, |tx, _| {
                            Ok::<_, Infallible>(
                                !remove_mempool_txs
                                    .contains(&tx.compute_txid()),
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
            sync_state.seq_message_queue.pop_front();
        }
        BlockHashEvent::Disconnected => {
            if !(block_hash_msg.block_hash == resp_block.hash
                && inner.mempool.chain.tip == resp_block.hash)
            {
                return Ok(());
            };
            for _tx_info in &resp_block.tx {
                // FIXME: insert without info
                let () = todo!();
            }
            inner.mempool.chain.tip = resp_block_parent;
            let () = inner
                .enforcer
                .disconnect_block(block_hash_msg.block_hash)
                .map_err(cusf_enforcer::Error::DisconnectBlock)?;
            sync_state.seq_message_queue.pop_front();
        }
    }
    Ok(())
}

// returns `true` if the tx was added or rejected successfully
async fn try_add_tx_from_cache<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    txid: &Txid,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return Ok(false);
    };
    let (mut value_in, value_out) = (Some(Amount::ZERO), Amount::ZERO);
    let mut input_txs_needed = LinkedHashSet::new();
    let mut input_txs = HashMap::<Txid, &Transaction>::new();
    for input in &tx.input {
        let OutPoint {
            txid: input_txid,
            vout,
        } = input.previous_output;
        let input_tx = if sync_state.rejected_txs.contains(&input_txid) {
            // Reject tx
            tracing::trace!("rejecting {txid}: rejected ancestor");
            sync_state.rejected_txs.insert(*txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(*txid));
            return Ok(true);
        } else if let Some(input_tx) = sync_state.tx_cache.get(&input_txid) {
            input_tx
        } else if let Some((input_tx, _)) = inner.mempool.txs.0.get(&input_txid)
        {
            input_tx
        } else {
            tracing::trace!("Need {input_txid} for {txid}");
            value_in = None;
            input_txs_needed.replace(input_txid);
            continue;
        };
        let value = input_tx.output[vout as usize].value;
        value_in = value_in.map(|value_in| value_in + value);
        input_txs.insert(input_txid, input_tx);
    }
    for input_txid in input_txs_needed.into_iter().rev() {
        sync_state
            .request_queue
            .push_front(RequestItem::Tx(input_txid, false))
    }
    let Some(value_in) = value_in else {
        return Ok(false);
    };
    let Some(fee_delta) = value_in.checked_sub(value_out) else {
        return Err(SyncTaskError::FeeOverflow);
    };
    if inner
        .enforcer
        .accept_tx(tx, &input_txs)
        .map_err(cusf_enforcer::Error::AcceptTx)?
    {
        inner.mempool.insert(tx.clone(), fee_delta.to_sat())?;
        tracing::trace!("added {txid} to mempool");
    } else {
        tracing::trace!("rejecting {txid}");
        sync_state.rejected_txs.insert(*txid);
        sync_state
            .request_queue
            .push_front(RequestItem::RejectTx(*txid));
    }
    let mempool_txs = inner.mempool.txs.0.len();
    tracing::debug!(%mempool_txs, "Syncing...");
    Ok(true)
}

// returns `true` if an item was applied successfully
async fn try_apply_next_seq_message<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
) -> Result<bool, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let res = 'res: {
        match sync_state.seq_message_queue.front() {
            Some(SequenceMessage::BlockHash(BlockHashMessage {
                block_hash,
                event: BlockHashEvent::Disconnected,
                ..
            })) => {
                if inner.mempool.chain.tip != *block_hash {
                    break 'res false;
                };
                let Some(block) = inner.mempool.chain.blocks.get(block_hash)
                else {
                    break 'res false;
                };
                for _tx_info in &block.tx {
                    // FIXME: insert without info
                    let () = todo!();
                }
                let () = inner
                    .enforcer
                    .disconnect_block(*block_hash)
                    .map_err(cusf_enforcer::Error::DisconnectBlock)?;
                inner.mempool.chain.tip = block
                    .previousblockhash
                    .unwrap_or_else(BlockHash::all_zeros);
                true
            }
            Some(SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Added,
                mempool_seq: _,
                zmq_seq: _,
            })) => {
                let txid = *txid;
                try_add_tx_from_cache(inner, sync_state, &txid).await?
            }
            Some(SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Removed,
                mempool_seq: _,
                zmq_seq: _,
            })) => {
                // FIXME: review -- looks sus
                inner.mempool.remove(txid)?.is_some()
            }
            Some(SequenceMessage::BlockHash(BlockHashMessage {
                event: BlockHashEvent::Connected,
                ..
            }))
            | None => false,
        }
    };
    if res {
        sync_state.seq_message_queue.pop_front();
    }
    Ok(res)
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
            // FIXME: remove
            tracing::debug!(%block.hash, "Handling block");
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
    while try_apply_next_seq_message(&mut inner_write, sync_state).await? {}
    Ok(())
}

async fn task<Enforcer, RpcClient>(
    inner: Arc<RwLock<MempoolSyncInner<Enforcer>>>,
    tx_cache: HashMap<Txid, Transaction>,
    rpc_client: RpcClient,
    sequence_stream: SequenceStream<'static>,
) -> Result<Infallible, SyncTaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
    RpcClient: bip300301::client::MainClient + Sync,
{
    // Filter mempool with enforcer
    let rejected_txs: LinkedHashSet<Txid> = {
        let mut inner_write = inner.write().await;
        let MempoolSyncInner {
            ref mut enforcer,
            ref mut mempool,
        } = *inner_write;
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
                enforcer.accept_tx(tx, &tx_inputs)
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
        seq_message_queue: VecDeque::new(),
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
        match combined_stream
            .next()
            .await
            .ok_or(SyncTaskError::CombinedStreamEnded)?
        {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () =
                    handle_seq_message(&inner, &mut sync_state, seq_msg?).await;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&inner, &mut sync_state, resp?).await?;
            }
        }
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
        RpcClient: bip300301::client::MainClient + Send + Sync + 'static,
        ErrHandler:
            FnOnce(SyncTaskError<Enforcer>) -> ErrHandlerFut + Send + 'static,
        ErrHandlerFut: Future<Output = ()> + Send,
    {
        let inner = MempoolSyncInner { enforcer, mempool };
        let inner = Arc::new(RwLock::new(inner));
        let inner_weak = Arc::downgrade(&inner);
        let task = spawn(async {
            let Err(err) =
                task(inner, tx_cache, rpc_client, sequence_stream).await;
            err_handler(err).await
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
        F: FnOnce(&Mempool, &Enforcer) -> Output,
    {
        let inner = self.inner.upgrade()?;
        let inner_read = inner.read().await;
        let res = f(&inner_read.mempool, &inner_read.enforcer);
        Some(res)
    }
}

impl<Enforcer> Drop for MempoolSync<Enforcer> {
    fn drop(&mut self) {
        self.task.abort()
    }
}
