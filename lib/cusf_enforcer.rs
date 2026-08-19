use std::{
    collections::HashSet, convert::Infallible, fmt::Debug, future::Future,
};

use bitcoin::{BlockHash, Transaction, Txid};
use educe::Educe;
use either::Either;
use futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use thiserror::Error;
use tracing::instrument;

// TODO: Enable specifying txs that can be restored to the mempool
#[derive(Clone, Debug)]
pub enum ConnectBlockAction {
    Accept { remove_mempool_txs: HashSet<Txid> },
    Reject,
}

impl Default for ConnectBlockAction {
    fn default() -> Self {
        Self::Accept {
            remove_mempool_txs: HashSet::new(),
        }
    }
}

// TODO: Enable specifying txs that can be restored to the mempool
#[derive(Clone, Debug, Default)]
pub struct DisconnectBlockAction {
    pub remove_mempool_txs: HashSet<Txid>,
}

#[derive(Clone, Debug)]
pub enum TxAcceptAction {
    Accept {
        /// Transactions that conflict with this one.
        /// It is not necessary to specify conflicts due to common inputs.
        conflicts_with: HashSet<Txid>,
        /// Tweak the weight by the specified value, in wu.
        /// The weight will saturate at zero and [`Weight::MAX`].
        weight_tweak: i64,
    },
    Reject,
}

/// Error attempting to sync a [`CusfEnforcer`] to a tip.
#[derive(Debug, Error)]
pub enum SyncToTipError<InvalidBlock, E> {
    /// The enforcer determined that a block is invalid, and stopped syncing
    /// before connecting it.
    /// `invalidateblock` will be called if this variant is encountered, and
    /// the sync will be re-attempted against the node's resulting tip.
    #[error("invalid block `{block_hash}`")]
    InvalidBlock {
        block_hash: BlockHash,
        #[source]
        reason: InvalidBlock,
    },
    /// Any other sync error
    #[error(transparent)]
    Other(#[from] E),
}

impl<InvalidBlock, E> SyncToTipError<InvalidBlock, E> {
    /// Map the error types, preserving the variant
    #[inline]
    pub fn map<InvalidBlock2, E2, F0, F1>(
        self,
        f0: F0,
        f1: F1,
    ) -> SyncToTipError<InvalidBlock2, E2>
    where
        F0: FnOnce(InvalidBlock) -> InvalidBlock2,
        F1: FnOnce(E) -> E2,
    {
        match self {
            Self::InvalidBlock { block_hash, reason } => {
                SyncToTipError::InvalidBlock {
                    block_hash,
                    reason: f0(reason),
                }
            }
            Self::Other(err) => SyncToTipError::Other(f1(err)),
        }
    }

    /// Map the invalid block reason error type, preserving the variant
    #[inline(always)]
    pub fn map_invalid_block_reason<InvalidBlock2, F>(
        self,
        f: F,
    ) -> SyncToTipError<InvalidBlock2, E>
    where
        F: FnOnce(InvalidBlock) -> InvalidBlock2,
    {
        self.map(
            f,
            #[inline(always)]
            |e| e,
        )
    }

    /// Map the error type, preserving the variant
    #[inline(always)]
    pub fn map_other<E2, F>(self, f: F) -> SyncToTipError<InvalidBlock, E2>
    where
        F: FnOnce(E) -> E2,
    {
        self.map(
            #[inline(always)]
            |reason| reason,
            f,
        )
    }
}

pub trait CusfEnforcer {
    type InvalidBlockReason: std::error::Error + Send + Sync + 'static;
    type SyncError: std::error::Error + Send + Sync + 'static;

    /// Attempt to sync to the specified tip.
    ///
    /// Implementations are not required to validate individual blocks while
    /// syncing. An implementation that does validate blocks, and rejects one
    /// as invalid, MUST NOT connect the invalid block. It should stop with
    /// its state synced to an ancestor of the invalid block, and return
    /// [`SyncToTipError::InvalidBlock`].
    fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        tip: BlockHash,
    ) -> impl Future<
        Output = Result<
            (),
            SyncToTipError<Self::InvalidBlockReason, Self::SyncError>,
        >,
    > + Send;

    type ConnectBlockError: std::error::Error + Send + Sync + 'static;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> impl Future<
        Output = Result<ConnectBlockAction, Self::ConnectBlockError>,
    > + Send;

    type DisconnectBlockError: std::error::Error + Send + Sync + 'static;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> impl Future<
        Output = Result<DisconnectBlockAction, Self::DisconnectBlockError>,
    > + Send;

    type AcceptTxError: std::error::Error + Send + Sync + 'static;

    /// Accept or reject a transaction, declaring conflicts for reasons other
    /// than shared inputs.
    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>;
}

#[derive(Debug, Error)]
pub enum FatalSyncToTipError<E> {
    #[error(transparent)]
    JsonRpc(#[from] bitcoin_jsonrpsee::jsonrpsee::core::ClientError),
    #[error(transparent)]
    Other(E),
}

/// General purpose error for [`CusfEnforcer`]
#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
pub enum Error<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error("CUSF Enforcer: error accepting tx")]
    AcceptTx(#[source] Enforcer::AcceptTxError),
    #[error("CUSF Enforcer: error connecting block")]
    ConnectBlock(#[source] Enforcer::ConnectBlockError),
    #[error("CUSF Enforcer: error disconnecting block")]
    DisconnectBlock(#[source] Enforcer::DisconnectBlockError),
    #[error("CUSF Enforcer: error during initial sync")]
    Sync(#[source] Enforcer::SyncError),
}

/// Sync the enforcer to the specified tip. If the enforcer reports a block as
/// invalid, `invalidateblock` is called on the node, and the sync is
/// re-attempted against the node's resulting tip. Returns the tip that was
/// finally synced to, which differs from the requested tip if any block was
/// invalidated. Errors are either the enforcer's own sync error, or a JSON-RPC
/// error from invalidating a block or fetching the node's tip.
pub async fn sync_to_tip<Enforcer, MainClient, Signal>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    shutdown_signal: Signal,
    mut tip: BlockHash,
) -> Result<BlockHash, FatalSyncToTipError<<Enforcer as CusfEnforcer>::SyncError>>
where
    Enforcer: CusfEnforcer,
    MainClient: bitcoin_jsonrpsee::client::MainClient + Sync,
    Signal: Future<Output = ()> + Send,
{
    let shutdown_signal = shutdown_signal.shared();
    let mut invalidated_blocks = HashSet::<BlockHash>::new();
    loop {
        match enforcer.sync_to_tip(shutdown_signal.clone(), tip).await {
            Ok(()) => return Ok(tip),
            Err(SyncToTipError::InvalidBlock { block_hash, reason }) => {
                assert!(
                    invalidated_blocks.insert(block_hash),
                    "enforcer reported block `{block_hash}` as invalid \
                     again, after it was already invalidated: {reason}"
                );
                tracing::warn!(
                    block_hash = %block_hash,
                    reason = %reason,
                    "invalidating block that the enforcer reported as \
                     invalid during sync"
                );
                let () = main_client
                    .invalidate_block(block_hash)
                    .await
                    .map_err(FatalSyncToTipError::JsonRpc)?;

                tip = main_client
                    .getbestblockhash()
                    .await
                    .map_err(FatalSyncToTipError::JsonRpc)?;
            }
            Err(SyncToTipError::Other(err)) => {
                return Err(FatalSyncToTipError::Other(err));
            }
        }
    }
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
pub enum InitialSyncError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error(transparent)]
    CusfEnforcer(<Enforcer as CusfEnforcer>::SyncError),
    #[error(transparent)]
    JsonRpc(#[from] bitcoin_jsonrpsee::jsonrpsee::core::ClientError),
    #[error(transparent)]
    SequenceStream(#[from] crate::zmq::SequenceStreamError),
    #[error("ZMQ sequence stream ended unexpectedly")]
    SequenceStreamEnded,
    #[error(transparent)]
    SubscribeSequence(#[from] crate::zmq::SubscribeSequenceError),
}

impl<Enforcer> From<FatalSyncToTipError<<Enforcer as CusfEnforcer>::SyncError>>
    for InitialSyncError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    fn from(
        err: FatalSyncToTipError<<Enforcer as CusfEnforcer>::SyncError>,
    ) -> Self {
        match err {
            FatalSyncToTipError::JsonRpc(err) => Self::JsonRpc(err),
            FatalSyncToTipError::Other(err) => Self::CusfEnforcer(err),
        }
    }
}

/// Subscribe to ZMQ sequence and sync enforcer, obtaining a ZMQ sequence
/// stream and best block hash
// 0. Subscribe to ZMQ sequence
// 1. Get best block hash
// 2. Sync enforcer to best block hash.
// 3. Get best block hash
// 4. If best block hash has changed, drop messages up to and including
//    (dis)connecting to best block hash, and go to step 2.
#[instrument(skip_all)]
pub async fn initial_sync<Enforcer, MainClient, Signal>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    zmq_addr_sequence: &str,
    shutdown_signal: Signal,
) -> Result<
    (BlockHash, crate::zmq::SequenceStream<'static>),
    InitialSyncError<Enforcer>,
>
where
    Enforcer: CusfEnforcer,
    MainClient: bitcoin_jsonrpsee::client::MainClient + Sync,
    Signal: Future<Output = ()> + Send,
{
    let mut sequence_stream =
        crate::zmq::subscribe_sequence(zmq_addr_sequence).await?;
    let mut block_hash = main_client.getbestblockhash().await?;
    tracing::debug!(
        block_hash = %block_hash,
        "fetched best block hash"
    );

    let block_header = main_client.getblockheader(block_hash).await?;

    let shutdown_signal = shutdown_signal.shared();

    let mut block_parent = block_header.prev_blockhash;
    'sync: loop {
        tracing::debug!(
            block_hash = %block_hash,
            block_height = block_header.height,
            "syncing enforcer to tip"
        );
        // If a block is invalidated here, the requested `block_hash` is no
        // longer in the node's active chain, so the tip-change handling below
        // drops the disconnect messages that the invalidation produced from
        // the sequence stream, and re-syncs against the node's new tip.
        let _synced_tip: BlockHash = sync_to_tip(
            enforcer,
            main_client,
            shutdown_signal.clone(),
            block_hash,
        )
        .await?;
        let best_block_hash = main_client.getbestblockhash().await?;
        if block_hash == best_block_hash {
            tracing::debug!(
                block_hash = %block_hash,
                block_height = block_header.height,
                "enforcer synced to tip!"
            );
            return Ok((block_hash, sequence_stream));
        }

        // We're NOT synced to the tip. Either the tip changed between starting
        // and finishing the sync, or a block was invalidated above, moving the
        // tip away from the chain we were syncing. Either way, we can expect
        // to read something from the sequence stream!
        'drop_seq_msgs: loop {
            tracing::trace!(
                "reading next ZMQ sequence message, looking for block hash"
            );
            let Some(msg) = sequence_stream.try_next().await? else {
                return Err(InitialSyncError::SequenceStreamEnded);
            };
            match msg {
                crate::zmq::SequenceMessage::BlockHash(block_hash_msg) => {
                    match block_hash_msg.event {
                        // A new block hash has been seen.
                        crate::zmq::BlockHashEvent::Connected => {
                            block_parent = block_hash;
                            block_hash = block_hash_msg.block_hash;
                        }
                        // While we were syncing the tip moved backwards. We need to backtrack
                        // until we reach the correct block.
                        crate::zmq::BlockHashEvent::Disconnected => {
                            block_hash = block_parent;
                            block_parent = main_client
                                .getblockheader(block_hash)
                                .await?
                                .prev_blockhash;
                        }
                    }
                    if block_hash == best_block_hash {
                        break 'drop_seq_msgs;
                    } else {
                        continue 'drop_seq_msgs;
                    }
                }
                // We want the next block hash, so loop back
                crate::zmq::SequenceMessage::TxHash(_) => {
                    continue 'drop_seq_msgs;
                }
            }
        }

        // We've obtained the most recent tip,
        tracing::debug!(
            block_hash = %block_hash,
            "looping back to sync with new tip"
        );
        continue 'sync;
    }
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
pub enum TaskError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error(transparent)]
    ConnectBlock(Enforcer::ConnectBlockError),
    #[error("Failed to decode block: `{block_hash}`")]
    DecodeBlock {
        block_hash: BlockHash,
        source: bitcoin::consensus::encode::Error,
    },
    #[error(transparent)]
    DisconnectBlock(Enforcer::DisconnectBlockError),
    #[error(transparent)]
    InitialSync(#[from] InitialSyncError<Enforcer>),
    #[error(transparent)]
    JsonRpc(#[from] bitcoin_jsonrpsee::jsonrpsee::core::ClientError),
    #[error(transparent)]
    ZmqSequence(#[from] crate::zmq::SequenceStreamError),
    #[error("ZMQ sequence stream ended unexpectedly")]
    ZmqSequenceEnded,
}

/// Run an enforcer in sync with a node
pub async fn task<Enforcer, MainClient, Signal>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    zmq_addr_sequence: &str,
    shutdown_signal: Signal,
) -> Result<(), TaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
    MainClient: bitcoin_jsonrpsee::client::MainClient + Sync,
    Signal: Future<Output = ()> + Send,
{
    use crate::zmq::{BlockHashEvent, BlockHashMessage, SequenceMessage};
    use bitcoin_jsonrpsee::client::{GetBlockClient as _, U8Witness};

    let shutdown_signal = shutdown_signal.shared();
    let (_best_block_hash, mut sequence_stream) = initial_sync(
        enforcer,
        main_client,
        zmq_addr_sequence,
        shutdown_signal.clone(),
    )
    .await?;

    // Pin the shutdown signal
    futures::pin_mut!(shutdown_signal);

    loop {
        let Some(sequence_msg) = tokio::select! {
            // borrow the shutdown signal, don't move
            _ = &mut shutdown_signal => {
                        tracing::info!("shutdown signal received, stopping");
                        return Ok(());
            }
            sequence_res = sequence_stream.try_next() => sequence_res
        }?
        else {
            return Err(TaskError::ZmqSequenceEnded);
        };

        let BlockHashMessage {
            block_hash, event, ..
        } = match sequence_msg {
            SequenceMessage::BlockHash(block_hash_msg) => block_hash_msg,
            SequenceMessage::TxHash(_) => continue,
        };
        match event {
            BlockHashEvent::Connected => {
                let block =
                    main_client.get_block(block_hash, U8Witness::<2>).await?;
                let block = (&block).try_into().map_err(|err| {
                    TaskError::DecodeBlock {
                        block_hash: block.hash,
                        source: err,
                    }
                })?;
                match enforcer
                    .connect_block(&block)
                    .map_err(TaskError::ConnectBlock)
                    .await?
                {
                    ConnectBlockAction::Accept {
                        remove_mempool_txs: _,
                    } => (),
                    ConnectBlockAction::Reject => {
                        main_client.invalidate_block(block_hash).await?;
                    }
                }
            }
            BlockHashEvent::Disconnected => {
                let DisconnectBlockAction {
                    remove_mempool_txs: _,
                } = enforcer
                    .disconnect_block(block_hash)
                    .map_err(TaskError::DisconnectBlock)
                    .await?;
            }
        }
    }
}

/// Connect block error for [`Compose`]
#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
pub enum ComposeConnectBlockError<C0, C1>
where
    C0: CusfEnforcer,
    C1: CusfEnforcer,
{
    #[error(transparent)]
    ConnectBlock(Either<C0::ConnectBlockError, C1::ConnectBlockError>),
    /// Blocks are disconnected from an enforcer if it accepts a block, and the
    /// other enforcer rejects it.
    #[error(transparent)]
    DisconnectBlock(Either<C0::DisconnectBlockError, C1::DisconnectBlockError>),
}

/// Compose two [`CusfEnforcer`]s, left-before-right
#[derive(Clone, Debug, Default)]
pub struct Compose<C0, C1>(pub(crate) C0, pub(crate) C1);

impl<C0, C1> Compose<C0, C1> {
    pub fn new(c0: C0, c1: C1) -> Self {
        Self(c0, c1)
    }
}

impl<C0, C1> CusfEnforcer for Compose<C0, C1>
where
    C0: CusfEnforcer + Send + 'static,
    C1: CusfEnforcer + Send + 'static,
{
    type InvalidBlockReason =
        Either<C0::InvalidBlockReason, C1::InvalidBlockReason>;
    type SyncError = Either<C0::SyncError, C1::SyncError>;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        block_hash: BlockHash,
    ) -> Result<(), SyncToTipError<Self::InvalidBlockReason, Self::SyncError>>
    {
        let shutdown_signal = shutdown_signal.shared();

        let () = self
            .0
            .sync_to_tip(shutdown_signal.clone(), block_hash)
            .map_err(|err| err.map(Either::Left, Either::Left))
            .await?;

        self.1
            .sync_to_tip(shutdown_signal, block_hash)
            .map_err(|err| err.map(Either::Right, Either::Right))
            .await
    }

    type ConnectBlockError = ComposeConnectBlockError<C0, C1>;

    async fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        let mut remove_mempool_txs = match self
            .0
            .connect_block(block)
            .map_err(|err| {
                Self::ConnectBlockError::ConnectBlock(Either::Left(err))
            })
            .await?
        {
            ConnectBlockAction::Accept { remove_mempool_txs } => {
                remove_mempool_txs
            }
            ConnectBlockAction::Reject => {
                return Ok(ConnectBlockAction::Reject);
            }
        };
        match self
            .1
            .connect_block(block)
            .map_err(|err| {
                Self::ConnectBlockError::ConnectBlock(Either::Right(err))
            })
            .await?
        {
            ConnectBlockAction::Accept {
                remove_mempool_txs: txs_right,
            } => {
                remove_mempool_txs.extend(txs_right);
                Ok(ConnectBlockAction::Accept { remove_mempool_txs })
            }
            ConnectBlockAction::Reject => {
                // Disconnect block on left enforcer
                let DisconnectBlockAction {
                    remove_mempool_txs: _,
                } = self
                    .0
                    .disconnect_block(block.block_hash())
                    .map_err(|err| {
                        Self::ConnectBlockError::DisconnectBlock(Either::Left(
                            err,
                        ))
                    })
                    .await?;
                Ok(ConnectBlockAction::Reject)
            }
        }
    }

    type DisconnectBlockError =
        Either<C0::DisconnectBlockError, C1::DisconnectBlockError>;

    async fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<DisconnectBlockAction, Self::DisconnectBlockError> {
        let mut res = self
            .0
            .disconnect_block(block_hash)
            .map_err(Either::Left)
            .await?;
        let DisconnectBlockAction { remove_mempool_txs } = self
            .1
            .disconnect_block(block_hash)
            .map_err(Either::Right)
            .await?;
        res.remove_mempool_txs.extend(remove_mempool_txs);
        Ok(res)
    }

    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<TxAcceptAction, Self::AcceptTxError> {
        match self.0.accept_tx(tx).map_err(Either::Left)? {
            TxAcceptAction::Accept {
                conflicts_with: left_conflicts,
                weight_tweak: left_weight_tweak,
            } => match self.1.accept_tx(tx).map_err(Either::Right)? {
                TxAcceptAction::Accept {
                    conflicts_with: right_conflicts,
                    weight_tweak: right_weight_tweak,
                } => {
                    let mut conflicts_with = left_conflicts;
                    conflicts_with.extend(right_conflicts);
                    let weight_tweak =
                        left_weight_tweak.saturating_add(right_weight_tweak);
                    Ok(TxAcceptAction::Accept {
                        conflicts_with,
                        weight_tweak,
                    })
                }
                TxAcceptAction::Reject => Ok(TxAcceptAction::Reject),
            },
            TxAcceptAction::Reject => Ok(TxAcceptAction::Reject),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultEnforcer;

impl CusfEnforcer for DefaultEnforcer {
    type InvalidBlockReason = Infallible;
    type SyncError = Infallible;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        _shutdown_signal: Signal,
        _block_hash: BlockHash,
    ) -> Result<(), SyncToTipError<Self::InvalidBlockReason, Self::SyncError>>
    {
        Ok(())
    }

    type ConnectBlockError = Infallible;

    async fn connect_block(
        &mut self,
        _block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        Ok(ConnectBlockAction::default())
    }

    type DisconnectBlockError = Infallible;

    async fn disconnect_block(
        &mut self,
        _block_hash: BlockHash,
    ) -> Result<DisconnectBlockAction, Self::DisconnectBlockError> {
        Ok(DisconnectBlockAction::default())
    }

    type AcceptTxError = Infallible;

    fn accept_tx(
        &mut self,
        _tx: &Transaction,
    ) -> Result<TxAcceptAction, Self::AcceptTxError> {
        Ok(TxAcceptAction::Accept {
            conflicts_with: HashSet::new(),
            weight_tweak: 0,
        })
    }
}

impl<C0, C1> CusfEnforcer for Either<C0, C1>
where
    C0: CusfEnforcer + Send,
    C1: CusfEnforcer + Send,
{
    type InvalidBlockReason =
        Either<C0::InvalidBlockReason, C1::InvalidBlockReason>;
    type SyncError = Either<C0::SyncError, C1::SyncError>;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        tip: BlockHash,
    ) -> Result<(), SyncToTipError<Self::InvalidBlockReason, Self::SyncError>>
    {
        let shutdown_signal = shutdown_signal.shared();
        match self {
            Self::Left(left) => {
                left.sync_to_tip(shutdown_signal, tip)
                    .map_err(|err| err.map(Either::Left, Either::Left))
                    .await
            }
            Self::Right(right) => {
                right
                    .sync_to_tip(shutdown_signal, tip)
                    .map_err(|err| err.map(Either::Right, Either::Right))
                    .await
            }
        }
    }

    type ConnectBlockError =
        Either<C0::ConnectBlockError, C1::ConnectBlockError>;

    async fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        match self {
            Self::Left(left) => {
                left.connect_block(block).map_err(Either::Left).await
            }
            Self::Right(right) => {
                right.connect_block(block).map_err(Either::Right).await
            }
        }
    }

    type DisconnectBlockError =
        Either<C0::DisconnectBlockError, C1::DisconnectBlockError>;

    async fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<DisconnectBlockAction, Self::DisconnectBlockError> {
        match self {
            Self::Left(left) => {
                left.disconnect_block(block_hash)
                    .map_err(Either::Left)
                    .await
            }
            Self::Right(right) => {
                right
                    .disconnect_block(block_hash)
                    .map_err(Either::Right)
                    .await
            }
        }
    }

    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<TxAcceptAction, Self::AcceptTxError> {
        match self {
            Self::Left(left) => left.accept_tx(tx).map_err(Either::Left),
            Self::Right(right) => right.accept_tx(tx).map_err(Either::Right),
        }
    }
}
