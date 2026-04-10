use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    task::{Poll, Waker},
    time::Instant,
};

use bitcoin::{BlockHash, Transaction, Txid};
use bitcoin_jsonrpsee::{
    client::{
        GetBlockClient as _, GetRawTransactionClient as _,
        GetRawTransactionVerbose, U8Witness,
    },
    jsonrpsee::core::{
        ClientError as JsonRpcError,
        params::{ArrayParams, BatchRequestBuilder, ObjectParams},
    },
};

use futures::{
    future::{self, FusedFuture, FutureExt as _},
    stream::{self, BoxStream, Stream, StreamExt as _},
};
use hashlink::LinkedHashSet;
use nonempty::NonEmpty;
use parking_lot::Mutex;
use thiserror::Error;

use crate::zmq::{SequenceMessage, SequenceStream, SequenceStreamError};

mod initial_sync;
pub(in crate::mempool) mod task;

pub use initial_sync::{
    SyncMempoolError as InitialSyncMempoolError, init_sync_mempool,
};
pub use task::MempoolSync;

/// Items requested while syncing
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RequestItem {
    Block(BlockHash),
    /// Reject a tx
    RejectTx(Txid),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Txid, bool),
}

/// Batched items requested while syncing
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BatchedRequestItem {
    BatchRejectTx(NonEmpty<Txid>),
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(NonEmpty<(Txid, bool)>),
    Single(RequestItem),
}

#[derive(Debug, Default)]
struct RequestQueueInner {
    queue: Mutex<LinkedHashSet<RequestItem>>,
    waker: Mutex<Option<Waker>>,
}

#[derive(Clone, Debug, Default)]
#[repr(transparent)]
struct RequestQueue {
    inner: Arc<RequestQueueInner>,
}

impl RequestQueue {
    /// Remove the request from the queue, if it exists
    fn remove(&self, request: &RequestItem) {
        self.inner.queue.lock().remove(request);
    }

    /// Push the request to the back, if it does not already exist
    fn push_back(&self, request: RequestItem) {
        self.inner.queue.lock().replace(request);
        if let Some(waker) = self.inner.waker.lock().take() {
            waker.wake()
        }
    }

    /// Push the request to the front, if it does not already exist
    fn push_front(&self, request: RequestItem) {
        let mut queue_lock = self.inner.queue.lock();
        queue_lock.replace(request);
        queue_lock.to_front(&request);
        if let Some(waker) = self.inner.waker.lock().take() {
            waker.wake()
        }
    }
}

impl Stream for RequestQueue {
    type Item = BatchedRequestItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut queue_lock = self.inner.queue.lock();
        *self.inner.waker.lock() = Some(cx.waker().clone());
        match queue_lock.pop_front() {
            Some(request @ RequestItem::Block(_)) => {
                Poll::Ready(Some(BatchedRequestItem::Single(request)))
            }
            Some(RequestItem::RejectTx(txid)) => {
                let mut txids = NonEmpty::new(txid);
                while let Some(&RequestItem::RejectTx(txid)) =
                    queue_lock.front()
                {
                    queue_lock.pop_front();
                    txids.push(txid);
                }
                let batched_request = if txids.tail.is_empty() {
                    BatchedRequestItem::Single(RequestItem::RejectTx(
                        txids.head,
                    ))
                } else {
                    BatchedRequestItem::BatchRejectTx(txids)
                };
                Poll::Ready(Some(batched_request))
            }
            Some(RequestItem::Tx(txid, in_mempool)) => {
                let mut txids = NonEmpty::new((txid, in_mempool));
                while let Some(&RequestItem::Tx(txid, in_mempool)) =
                    queue_lock.front()
                {
                    queue_lock.pop_front();
                    txids.push((txid, in_mempool));
                }
                let batched_request = if txids.tail.is_empty() {
                    let (txid, in_mempool) = txids.head;
                    BatchedRequestItem::Single(RequestItem::Tx(
                        txid, in_mempool,
                    ))
                } else {
                    BatchedRequestItem::BatchTx(txids)
                };
                Poll::Ready(Some(batched_request))
            }
            None => Poll::Pending,
        }
    }
}

/// Head of the sequence message queue.
/// Includes the time at which the message reached the head of the queue.
#[derive(Debug)]
struct SeqMessageQueueHead {
    msg: SequenceMessage,
    /// The time at which the message reached the head of the queue.
    reached_head_time: Instant,
}

/// Queue of sequence messages.
/// Tracks how long a message has been at the head of the queue.
#[derive(Debug, Default)]
struct SeqMessageQueue {
    head: Option<SeqMessageQueueHead>,
    tail: VecDeque<SequenceMessage>,
}

impl SeqMessageQueue {
    fn front(&self) -> &Option<SeqMessageQueueHead> {
        &self.head
    }

    fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    fn pop_front(&mut self) -> Option<SequenceMessage> {
        let Self { head, tail } = self;
        let new_head = tail.pop_front().map(|msg| SeqMessageQueueHead {
            msg,
            reached_head_time: Instant::now(),
        });
        std::mem::replace(head, new_head).map(|old_head| old_head.msg)
    }

    fn push_back(&mut self, msg: SequenceMessage) {
        let Self { head, tail } = self;
        if head.is_none() {
            assert!(tail.is_empty());
            let reached_head_time = Instant::now();
            *head = Some(SeqMessageQueueHead {
                msg,
                reached_head_time,
            });
        } else {
            tail.push_back(msg);
        }
    }
}

impl FromIterator<SequenceMessage> for SeqMessageQueue {
    fn from_iter<T>(msgs: T) -> Self
    where
        T: IntoIterator<Item = SequenceMessage>,
    {
        let mut res = Self::default();
        for msg in msgs.into_iter() {
            res.push_back(msg)
        }
        res
    }
}

/// Responses received while syncing
#[derive(Clone, Debug)]
enum ResponseItem {
    Block(Box<bitcoin_jsonrpsee::client::Block<true>>),
    RejectTx,
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    Tx(Box<Transaction>, bool),
}

/// Responses received while syncing
#[derive(Clone, Debug)]
enum BatchedResponseItem {
    BatchRejectTx,
    /// Bool indicating if the tx is a mempool tx.
    /// `false` if the tx is needed as a dependency for a mempool tx
    BatchTx(Vec<(Transaction, bool)>),
    Single(ResponseItem),
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("Error deserializing tx")]
    DeserializeTx(#[from] bitcoin::consensus::encode::FromHexError),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
}

async fn batched_request<RpcClient>(
    rpc_client: &RpcClient,
    request: BatchedRequestItem,
) -> Result<BatchedResponseItem, RequestError>
where
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Sync,
{
    const NEGATIVE_MAX_SATS: i64 = -(21_000_000 * 100_000_000);
    match request {
        BatchedRequestItem::BatchRejectTx(txs) => {
            let mut request = BatchRequestBuilder::new();
            for txid in txs {
                let mut params = ObjectParams::new();
                params.insert("txid", txid).unwrap();
                // set priority fee to extremely negative so that it is cleared
                // from mempool as soon as possible
                params.insert("fee_delta", NEGATIVE_MAX_SATS).unwrap();
                request.insert("prioritisetransaction", params).unwrap();
            }
            let _resp: Vec<bool> = rpc_client
                .batch_request(request)
                // Must box due to https://github.com/rust-lang/rust/issues/100013
                .boxed()
                .await?
                .into_ok()
                .map_err(|mut errs| JsonRpcError::from(errs.next().unwrap()))?
                .collect();
            Ok(BatchedResponseItem::BatchRejectTx)
        }
        BatchedRequestItem::BatchTx(txs) => {
            let in_mempool = HashMap::<_, _>::from_iter(txs.iter().copied());
            let mut request = BatchRequestBuilder::new();
            for (txid, _) in txs {
                let mut params = ArrayParams::new();
                params.insert(txid).unwrap();
                params.insert(false).unwrap();
                request.insert("getrawtransaction", params).unwrap();
            }
            let txs: Vec<(Transaction, bool)> = rpc_client
                .batch_request(request)
                // Must box due to https://github.com/rust-lang/rust/issues/100013
                .boxed()
                .await?
                .into_ok()
                .map_err(|mut errs| JsonRpcError::from(errs.next().unwrap()))?
                .map(|tx_hex: String| {
                    bitcoin::consensus::encode::deserialize_hex(&tx_hex).map(
                        |tx: Transaction| {
                            let txid = tx.compute_txid();
                            (tx, in_mempool[&txid])
                        },
                    )
                })
                .collect::<Result<_, _>>()?;
            Ok(BatchedResponseItem::BatchTx(txs))
        }
        BatchedRequestItem::Single(RequestItem::Block(block_hash)) => {
            let block =
                rpc_client.get_block(block_hash, U8Witness::<2>).await?;
            let resp = ResponseItem::Block(Box::new(block));
            Ok(BatchedResponseItem::Single(resp))
        }
        BatchedRequestItem::Single(RequestItem::RejectTx(txid)) => {
            // set priority fee to extremely negative so that it is cleared
            // from mempool as soon as possible
            let _: bool = rpc_client
                .prioritize_transaction(txid, NEGATIVE_MAX_SATS)
                .await?;
            let resp = ResponseItem::RejectTx;
            Ok(BatchedResponseItem::Single(resp))
        }
        BatchedRequestItem::Single(RequestItem::Tx(txid, in_mempool)) => {
            let tx_hex = rpc_client
                .get_raw_transaction(
                    txid,
                    GetRawTransactionVerbose::<false>,
                    None,
                )
                .await?;
            let tx: Transaction =
                bitcoin::consensus::encode::deserialize_hex(&tx_hex)?;
            let resp = ResponseItem::Tx(Box::new(tx), in_mempool);
            Ok(BatchedResponseItem::Single(resp))
        }
    }
}

type ResponseStreamItem = Result<BatchedResponseItem, RequestError>;

/// Items processed while syncing
#[derive(Debug)]
#[must_use]
enum CombinedStreamItem {
    ZmqSeq(Result<SequenceMessage, SequenceStreamError>),
    Response(ResponseStreamItem),
    /// Timeout while waiting to apply next sequence message
    ApplySeqMessageTimeout,
    /// The sync was stopped
    Shutdown,
}

/// Polls streams in a round-robin manner
struct CombinedStream<
    'sequence_msgs,
    'responses,
    ApplySeqMsgTimeout,
    ShutdownSignal,
> {
    pub sequence_msgs: stream::Fuse<SequenceStream<'sequence_msgs>>,
    pub responses: stream::Fuse<BoxStream<'responses, ResponseStreamItem>>,
    pub apply_seq_msg_timeout: future::Fuse<ApplySeqMsgTimeout>,
    pub shutdown_signal: future::Fuse<ShutdownSignal>,
    position: u8,
}

impl<'sequence_msgs, 'responses, ApplySeqMessageTimeout, ShutdownSignal>
    CombinedStream<
        'sequence_msgs,
        'responses,
        ApplySeqMessageTimeout,
        ShutdownSignal,
    >
where
    ApplySeqMessageTimeout: Future<Output = ()>,
    ShutdownSignal: Future<Output = ()>,
{
    fn new(
        sequence_msgs: SequenceStream<'sequence_msgs>,
        responses: BoxStream<'responses, ResponseStreamItem>,
        apply_seq_msg_timeout: ApplySeqMessageTimeout,
        shutdown_signal: ShutdownSignal,
    ) -> Self {
        Self {
            sequence_msgs: sequence_msgs.fuse(),
            responses: responses.fuse(),
            apply_seq_msg_timeout: apply_seq_msg_timeout.fuse(),
            shutdown_signal: shutdown_signal.fuse(),
            position: 0,
        }
    }

    fn is_done(&self) -> bool {
        let Self {
            sequence_msgs,
            responses,
            apply_seq_msg_timeout,
            shutdown_signal,
            position: _,
        } = self;
        sequence_msgs.is_done()
            && responses.is_done()
            && apply_seq_msg_timeout.is_terminated()
            && shutdown_signal.is_terminated()
    }
}

impl<'sequence_msgs, 'responses, ApplySeqMessageTimeout, ShutdownSignal> Stream
    for CombinedStream<
        'sequence_msgs,
        'responses,
        ApplySeqMessageTimeout,
        ShutdownSignal,
    >
where
    ApplySeqMessageTimeout: Future<Output = ()> + Send + Unpin,
    ShutdownSignal: Future<Output = ()> + Send + Unpin,
{
    type Item = CombinedStreamItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut attempts = 0;
        while attempts < 4 {
            match self.position {
                0 => {
                    if !self.sequence_msgs.is_done()
                        && let Poll::Ready(Some(res)) =
                            self.sequence_msgs.poll_next_unpin(cx)
                    {
                        self.position = 1;
                        return Poll::Ready(Some(CombinedStreamItem::ZmqSeq(
                            res,
                        )));
                    }
                }
                1 => {
                    if !self.responses.is_done()
                        && let Poll::Ready(Some(res)) =
                            self.responses.poll_next_unpin(cx)
                    {
                        self.position = 2;
                        return Poll::Ready(Some(
                            CombinedStreamItem::Response(res),
                        ));
                    }
                }
                2 => {
                    if !self.apply_seq_msg_timeout.is_terminated()
                        && let Poll::Ready(()) =
                            self.apply_seq_msg_timeout.poll_unpin(cx)
                    {
                        self.position = 3;
                        return Poll::Ready(Some(
                            CombinedStreamItem::ApplySeqMessageTimeout,
                        ));
                    }
                }
                3 => {
                    if !self.shutdown_signal.is_terminated()
                        && let Poll::Ready(()) =
                            self.shutdown_signal.poll_unpin(cx)
                    {
                        self.position = 0;
                        return Poll::Ready(Some(CombinedStreamItem::Shutdown));
                    }
                }
                _ => unreachable!(),
            }
            attempts += 1;
            self.position = (self.position + 1) % 4;
        }
        if self.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
