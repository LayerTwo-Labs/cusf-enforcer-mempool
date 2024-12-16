//! Initial mempool sync

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    convert::Infallible,
};

use bip300301::{
    bitcoin::hashes::Hash as _,
    client::{BoolWitness, GetRawMempoolClient as _, RawMempoolWithSequence},
    jsonrpsee::core::ClientError as JsonRpcError,
};
use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid};
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

fn connect_block<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    block: &bip300301::client::Block<true>,
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

fn disconnect_block<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    block: &bip300301::client::Block<true>,
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

fn handle_resp_block<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
    resp_block: bip300301::client::Block<true>,
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
            let () = connect_block(sync_state, &resp_block)?;
            sync_state.seq_message_queue.pop_front();
        }
        Some(SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        })) if *block_hash == resp_block.hash
            && sync_state.mempool.chain.tip == resp_block.hash =>
        {
            let () = disconnect_block(sync_state, &resp_block)?;
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
    let (mut value_in, value_out) = (Some(Amount::ZERO), Amount::ZERO);
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
        value_in = value_in.map(|value_in| value_in + value);
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
fn try_apply_next_seq_message<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
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
                let () = disconnect_block(sync_state, &block)?;
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

fn handle_resp<Enforcer>(
    sync_state: &mut MempoolSyncing<Enforcer>,
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
            let () = handle_resp_block(sync_state, *block)?;
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
    while try_apply_next_seq_message(sync_state)? {}
    Ok(())
}

/// Returns the zmq sequence stream, synced mempool, and the accumulated tx cache
pub async fn init_sync_mempool<'a, Enforcer, RpcClient>(
    enforcer: &mut Enforcer,
    rpc_client: &RpcClient,
    zmq_addr_sequence: &str,
) -> Result<
    (SequenceStream<'a>, Mempool, HashMap<Txid, Transaction>),
    SyncMempoolError<Enforcer>,
>
where
    Enforcer: CusfEnforcer,
    RpcClient: bip300301::client::MainClient + Sync,
{
    let (best_block_hash, sequence_stream) =
        cusf_enforcer::initial_sync(enforcer, rpc_client, zmq_addr_sequence)
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

    let mut combined_stream = stream::select(
        sequence_stream.map(CombinedStreamItem::ZmqSeq),
        response_stream.map(CombinedStreamItem::Response),
    );
    while !sync_state.is_synced() {
        // FIXME: remove
        tracing::debug!(
            "sync state needed: {} blocks, {} txs",
            sync_state.blocks_needed.len(),
            sync_state.txs_needed.len()
        );
        match combined_stream
            .next()
            .await
            .ok_or(SyncMempoolError::CombinedStreamEnded)?
        {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let () = handle_seq_message(&mut sync_state, seq_msg?)?;
            }
            CombinedStreamItem::Response(resp) => {
                let () = handle_resp(&mut sync_state, resp?)?;
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
        let (sequence_stream, _resp_stream) = combined_stream.into_inner();
        sequence_stream.into_inner()
    };
    Ok((sequence_stream, mempool, tx_cache))
}
