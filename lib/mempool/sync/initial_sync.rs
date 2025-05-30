//! Initial mempool sync

use core::future::Future;
use futures::FutureExt as _;
use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    convert::Infallible,
};

use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid};
use bitcoin_jsonrpsee::{
    bitcoin::hashes::Hash as _,
    client::{BoolWitness, GetRawMempoolClient as _, RawMempoolWithSequence},
    jsonrpsee::core::ClientError as JsonRpcError,
};
use educe::Educe;
use futures::{stream, StreamExt as _};
use hashlink::LinkedHashSet;
use imbl::HashSet;
use thiserror::Error;

use super::{
    super::{Mempool, MempoolInsertError, MempoolRemoveError},
    batched_request, BatchedResponseItem, CombinedStreamItem, RequestError,
    RequestItem, RequestQueue, ResponseItem,
};
use crate::{
    cusf_enforcer::{self, ConnectBlockAction, CusfEnforcer},
    zmq::{
        BlockHashEvent, BlockHashMessage, SequenceMessage, SequenceStream,
        SequenceStreamError, TxHashEvent, TxHashMessage,
    },
};

/// Actions to take immediately after initial sync is completed
#[derive(Debug, Default)]
struct PostSync {
    /// Mempool txs excluded by enforcer
    remove_mempool_txs: HashSet<Txid>,
}

impl PostSync {
    fn apply(self, mempool: &mut Mempool) -> Result<(), MempoolRemoveError> {
        let _removed_txs = mempool
            .try_filter(true, |tx, _| {
                Ok::<_, Infallible>(
                    !self.remove_mempool_txs.contains(&tx.compute_txid()),
                )
            })
            .map_err(|err| {
                let either::Either::Left(err) = err;
                err
            })?;
        Ok(())
    }
}

#[derive(Debug)]
struct MempoolSyncing<'a, Enforcer> {
    blocks_needed: LinkedHashSet<BlockHash>,
    enforcer: &'a mut Enforcer,
    /// Drop messages with lower mempool sequences.
    /// Set to None after encountering this mempool sequence ID.
    /// Return an error if higher sequence is encountered.
    first_mempool_sequence: Option<u64>,
    mempool: Mempool,
    post_sync: PostSync,
    request_queue: RequestQueue,
    seq_message_queue: VecDeque<SequenceMessage>,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
    txs_needed: LinkedHashSet<Txid>,
}

impl<Enforcer> MempoolSyncing<'_, Enforcer> {
    fn is_synced(&self) -> bool {
        self.blocks_needed.is_empty()
            && self.txs_needed.is_empty()
            && self.seq_message_queue.is_empty()
    }
}

#[derive(Educe)]
#[educe(Debug(bound(cusf_enforcer::Error<Enforcer>: std::fmt::Debug)))]
#[derive(Error)]
pub enum SyncMempoolError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    // TODO - this is not really an error...
    #[error("Sync was stopped")]
    Shutdown,
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
    #[error("Request error")]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
}

async fn connect_block<Enforcer>(
    sync_state: &mut MempoolSyncing<'_, Enforcer>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    for tx_info in &block.tx {
        let txid = tx_info.txid;
        let _removed: Option<_> = sync_state.mempool.remove(&txid)?;
        sync_state.txs_needed.remove(&txid);
        sync_state
            .request_queue
            .remove(&RequestItem::Tx(txid, true));
    }
    let block_decoded =
        block
            .try_into()
            .map_err(|err| SyncMempoolError::DecodeBlock {
                block_hash: block.hash,
                source: err,
            })?;
    match sync_state
        .enforcer
        .connect_block(&block_decoded)
        .await
        .map_err(cusf_enforcer::Error::ConnectBlock)?
    {
        ConnectBlockAction::Accept { remove_mempool_txs } => {
            sync_state
                .post_sync
                .remove_mempool_txs
                .extend(remove_mempool_txs);
        }
        ConnectBlockAction::Reject => {
            // FIXME: reject block
        }
    };
    sync_state.mempool.chain.tip = block.hash;
    Ok(())
}

async fn disconnect_block<Enforcer>(
    sync_state: &mut MempoolSyncing<'_, Enforcer>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    for _tx_info in &block.tx {
        // FIXME: insert without info
        let () = todo!();
    }
    let () = sync_state
        .enforcer
        .disconnect_block(block.hash)
        .await
        .map_err(cusf_enforcer::Error::DisconnectBlock)?;
    sync_state.mempool.chain.tip =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    Ok(())
}

fn handle_block_hash_msg<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    block_hash_msg: BlockHashMessage,
) {
    let BlockHashMessage {
        block_hash,
        event: _,
        ..
    } = block_hash_msg;
    if !sync_state.mempool.chain.blocks.contains_key(&block_hash) {
        tracing::trace!(%block_hash, "Adding block to req queue");
        sync_state.blocks_needed.replace(block_hash);
        sync_state
            .request_queue
            .push_back(RequestItem::Block(block_hash));
    }
}

fn handle_tx_hash_msg<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    tx_hash_msg: TxHashMessage,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
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
                return Err(SyncMempoolError::FirstMempoolSequence(
                    first_mempool_seq,
                ))
            }
        }
    }
    match event {
        TxHashEvent::Added => {
            tracing::trace!(%txid, "Added tx to req queue");
            sync_state.txs_needed.replace(txid);
            sync_state
                .request_queue
                .push_back(RequestItem::Tx(txid, true));
        }
        TxHashEvent::Removed => {
            tracing::trace!(%txid, "Removed tx from req queue");
            sync_state.txs_needed.remove(&txid);
            sync_state
                .request_queue
                .remove(&RequestItem::Tx(txid, true));
        }
    }
    Ok(())
}

fn handle_seq_message<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    seq_msg: SequenceMessage,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(block_hash_msg) => {
            let () = handle_block_hash_msg(sync_state, block_hash_msg);
        }
        SequenceMessage::TxHash(tx_hash_msg) => {
            let () = handle_tx_hash_msg(sync_state, tx_hash_msg)?;
        }
    }
    sync_state.seq_message_queue.push_back(seq_msg);
    Ok(())
}

async fn handle_resp_block<Enforcer>(
    sync_state: &mut MempoolSyncing<'_, Enforcer>,
    resp_block: bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    sync_state.blocks_needed.remove(&resp_block.hash);
    match sync_state.seq_message_queue.front() {
        Some(SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Connected,
            ..
        })) if *block_hash == resp_block.hash => {
            let () = connect_block(sync_state, &resp_block).await?;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        })) if *block_hash == resp_block.hash
            && sync_state.mempool.chain.tip == resp_block.hash =>
        {
            let () = disconnect_block(sync_state, &resp_block).await?;
            sync_state.seq_message_queue.pop_front();
        }
        Some(_) | None => (),
    }
    sync_state
        .mempool
        .chain
        .blocks
        .insert(resp_block.hash, resp_block);
    Ok(())
}

fn handle_resp_tx<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    tx: Transaction,
) {
    let txid = tx.compute_txid();
    sync_state.txs_needed.remove(&txid);
    sync_state.tx_cache.insert(txid, tx);
}

// returns `true` if the tx was added successfully
fn try_add_tx_from_cache<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    txid: &Txid,
) -> Result<bool, SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
{
    let Some(tx) = sync_state.tx_cache.get(txid) else {
        return Ok(false);
    };
    let mut value_in = Some(Amount::ZERO);
    let mut input_txs_needed = Vec::new();
    for input in &tx.input {
        let OutPoint {
            txid: input_txid,
            vout,
        } = input.previous_output;
        let input_tx =
            if let Some(input_tx) = sync_state.tx_cache.get(&input_txid) {
                input_tx
            } else if let Some((input_tx, _)) =
                sync_state.mempool.txs.0.get(&input_txid)
            {
                input_tx
            } else {
                tracing::trace!("Need {input_txid} for {txid}");
                value_in = None;
                input_txs_needed.push(input_txid);
                continue;
            };
        let value = input_tx.output[vout as usize].value;
        value_in = value_in.and_then(|value_in| value_in.checked_add(value));
    }
    for input_txid in input_txs_needed.into_iter().rev() {
        sync_state.txs_needed.replace(input_txid);
        sync_state.txs_needed.to_front(&input_txid);
        sync_state
            .request_queue
            .push_front(RequestItem::Tx(input_txid, false))
    }
    let Some(value_in) = value_in else {
        return Ok(false);
    };
    let mut value_out = Some(Amount::ZERO);
    for output in &tx.output {
        value_out =
            value_out.and_then(|value_out| value_out.checked_add(output.value));
    }
    let Some(value_out) = value_out else {
        return Ok(false);
    };
    let Some(fee_delta) = value_in.checked_sub(value_out) else {
        return Err(SyncMempoolError::FeeOverflow);
    };
    sync_state.mempool.insert(tx.clone(), fee_delta.to_sat())?;
    tracing::trace!("added {txid} to mempool");
    let mempool_txs = sync_state.mempool.txs.0.len();
    tracing::debug!(%mempool_txs, "Syncing...");
    Ok(true)
}

// returns `true` if an item was applied successfully
async fn try_apply_next_seq_message<Enforcer>(
    sync_state: &mut MempoolSyncing<'_, Enforcer>,
) -> Result<bool, SyncMempoolError<Enforcer>>
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
                if sync_state.mempool.chain.tip != *block_hash {
                    break 'res false;
                };
                let Some(block) =
                    sync_state.mempool.chain.blocks.get(block_hash).cloned()
                else {
                    break 'res false;
                };
                let () = disconnect_block(sync_state, &block).await?;
                true
            }
            Some(SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Added,
                mempool_seq: _,
                zmq_seq: _,
            })) => {
                let txid = *txid;
                try_add_tx_from_cache(sync_state, &txid)?
            }
            Some(SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Removed,
                mempool_seq: _,
                zmq_seq: _,
            })) => {
                // FIXME: review -- looks sus
                sync_state.mempool.remove(txid)?.is_some()
            }
            Some(SequenceMessage::BlockHash(_)) | None => false,
        }
    };
    if res {
        sync_state.seq_message_queue.pop_front();
    }
    Ok(res)
}

async fn handle_resp<Enforcer>(
    sync_state: &mut MempoolSyncing<'_, Enforcer>,
    resp: BatchedResponseItem,
) -> Result<(), SyncMempoolError<Enforcer>>
where
    Enforcer: CusfEnforcer,
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
            tracing::debug!(%block.hash, "Handling block");
            let () = handle_resp_block(sync_state, *block).await?;
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
        | BatchedResponseItem::Single(ResponseItem::RejectTx) => {}
    }
    while try_apply_next_seq_message(sync_state).await? {}
    Ok(())
}

/// Returns the zmq sequence stream, synced mempool, and the accumulated tx cache
pub async fn init_sync_mempool<
    'a,
    Signal: Future<Output = ()> + Send,
    Enforcer,
    RpcClient,
>(
    enforcer: &mut Enforcer,
    rpc_client: &RpcClient,
    zmq_addr_sequence: &str,
    shutdown_signal: Signal, // Would it be better to return a Some/None, indicating sync stoppage?
) -> Result<
    (SequenceStream<'a>, Mempool, HashMap<Txid, Transaction>),
    SyncMempoolError<Enforcer>,
>
where
    Enforcer: CusfEnforcer,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Sync,
{
    let shutdown_signal = shutdown_signal.shared();
    let (best_block_hash, sequence_stream) = cusf_enforcer::initial_sync(
        enforcer,
        rpc_client,
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
    let mut sync_state = {
        let request_queue = RequestQueue::default();
        request_queue.push_back(RequestItem::Block(best_block_hash));
        for txid in &txids {
            request_queue.push_back(RequestItem::Tx(*txid, true));
        }
        let seq_message_queue = VecDeque::from_iter(txids.iter().map(|txid| {
            SequenceMessage::TxHash(TxHashMessage {
                txid: *txid,
                event: TxHashEvent::Added,
                mempool_seq: mempool_sequence,
                zmq_seq: 0,
            })
        }));
        MempoolSyncing {
            blocks_needed: LinkedHashSet::from_iter([best_block_hash]),
            enforcer,
            first_mempool_sequence: Some(mempool_sequence + 1),
            mempool: Mempool::new(best_block_hash),
            post_sync: PostSync::default(),
            request_queue,
            seq_message_queue,
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::from_iter(txids),
        }
    };

    let response_stream = sync_state
        .request_queue
        .clone()
        .then(|request| batched_request(rpc_client, request))
        .boxed();

    // Pin the shutdown signal
    futures::pin_mut!(shutdown_signal);
    let shutdown_stream = shutdown_signal.into_stream();

    // This is kinda wonky - but the select() function is only able to operate on
    // two streams. There's the stream_select! macro, but that consumes the streams.
    // We need to retrieve them below, therefore the nested stream::select()
    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        stream::select(
            response_stream.map(CombinedStreamItem::Response),
            shutdown_stream.map(|_| CombinedStreamItem::Shutdown),
        ),
    );
    while !sync_state.is_synced() {
        match combined_stream
            .next()
            .await
            .ok_or(SyncMempoolError::CombinedStreamEnded)?
        {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () = handle_seq_message(&mut sync_state, seq_msg?)?;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&mut sync_state, resp?).await?;
            }
            CombinedStreamItem::Shutdown => {
                return Err(SyncMempoolError::Shutdown);
            }
        }
    }
    let MempoolSyncing {
        mut mempool,
        post_sync,
        tx_cache,
        ..
    } = sync_state;
    let () = post_sync.apply(&mut mempool)?;
    let sequence_stream = {
        let (sequence_stream, _) = combined_stream.into_inner();
        sequence_stream.into_inner()
    };
    Ok((sequence_stream, mempool, tx_cache))
}
