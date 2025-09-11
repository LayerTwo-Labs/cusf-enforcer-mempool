use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt::Debug,
    future::Future,
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
    },
    Reject,
}

pub trait CusfEnforcer {
    type SyncError: std::error::Error + Send + Sync + 'static;

    /// Attempt to sync to the specified tip
    fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        tip: BlockHash,
    ) -> impl Future<Output = Result<(), Self::SyncError>> + Send;

    type ConnectBlockError: std::error::Error + Send + Sync + 'static;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> impl Future<Output = Result<ConnectBlockAction, Self::ConnectBlockError>>
           + Send;

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
    /// Inputs to a tx are always available.
    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>;
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

/// Subscribe to ZMQ sequence and sync enforcer, obtaining a ZMQ sequence
/// stream and best block hash
// 0. Subscribe to ZMQ sequence
// 1. Get best block hash
// 2. Sync enforcer to best block hash.
// 3. Get best block hash
// 4. If best block hash has changed, drop messages up to and including
//    (dis)connecting to best block hash, and go to step 2.
#[instrument(skip_all)]
pub async fn initial_sync<'a, Enforcer, MainClient, Signal>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    zmq_addr_sequence: &str,
    shutdown_signal: Signal,
) -> Result<
    (BlockHash, crate::zmq::SequenceStream<'a>),
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
        let () = enforcer
            .sync_to_tip(shutdown_signal.clone(), block_hash)
            .map_err(InitialSyncError::CusfEnforcer)
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

        // We're NOT synced to the tip. This means that between we started the sync
        // and finished, the tip has changed. That means we can expect to read something
        // from the sequence stream!
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
#[derive(Debug, Default)]
pub struct Compose<C0, C1>(pub(crate) C0, pub(crate) C1);

impl<C0, C1> CusfEnforcer for Compose<C0, C1>
where
    C0: CusfEnforcer + Send + 'static,
    C1: CusfEnforcer + Send + 'static,
{
    type SyncError = Either<C0::SyncError, C1::SyncError>;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        block_hash: BlockHash,
    ) -> Result<(), Self::SyncError> {
        let shutdown_signal = shutdown_signal.shared();

        let () = self
            .0
            .sync_to_tip(shutdown_signal.clone(), block_hash)
            .map_err(Either::Left)
            .await?;

        self.1
            .sync_to_tip(shutdown_signal, block_hash)
            .map_err(Either::Right)
            .await
    }

    type ConnectBlockError = ComposeConnectBlockError<C0, C1>;

    async fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        let res_left = self
            .0
            .connect_block(block)
            .map_err(|err| {
                Self::ConnectBlockError::ConnectBlock(Either::Left(err))
            })
            .await?;
        let res_right = self
            .1
            .connect_block(block)
            .map_err(|err| {
                Self::ConnectBlockError::ConnectBlock(Either::Right(err))
            })
            .await?;
        match (res_left, res_right) {
            (
                ConnectBlockAction::Accept {
                    mut remove_mempool_txs,
                },
                ConnectBlockAction::Accept {
                    remove_mempool_txs: txs_right,
                },
            ) => {
                remove_mempool_txs.extend(txs_right);
                Ok(ConnectBlockAction::Accept { remove_mempool_txs })
            }
            (
                ConnectBlockAction::Reject,
                ConnectBlockAction::Accept {
                    remove_mempool_txs: _,
                },
            ) => {
                // Disconnect block on right enforcer
                let DisconnectBlockAction {
                    remove_mempool_txs: _,
                } = self
                    .1
                    .disconnect_block(block.block_hash())
                    .map_err(|err| {
                        Self::ConnectBlockError::DisconnectBlock(Either::Right(
                            err,
                        ))
                    })
                    .await?;
                Ok(ConnectBlockAction::Reject)
            }
            (
                ConnectBlockAction::Accept {
                    remove_mempool_txs: _,
                },
                ConnectBlockAction::Reject,
            ) => {
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
            (ConnectBlockAction::Reject, ConnectBlockAction::Reject) => {
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

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        match self.0.accept_tx(tx, tx_inputs).map_err(Either::Left)? {
            TxAcceptAction::Accept {
                conflicts_with: left_conflicts,
            } => {
                match self.1.accept_tx(tx, tx_inputs).map_err(Either::Right)? {
                    TxAcceptAction::Accept {
                        conflicts_with: right_conflicts,
                    } => {
                        let mut conflicts_with = left_conflicts;
                        conflicts_with.extend(right_conflicts);
                        Ok(TxAcceptAction::Accept { conflicts_with })
                    }
                    TxAcceptAction::Reject => Ok(TxAcceptAction::Reject),
                }
            }
            TxAcceptAction::Reject => Ok(TxAcceptAction::Reject),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultEnforcer;

impl CusfEnforcer for DefaultEnforcer {
    type SyncError = Infallible;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        _shutdown_signal: Signal,
        _block_hash: BlockHash,
    ) -> Result<(), Self::SyncError> {
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

    fn accept_tx<TxRef>(
        &mut self,
        _tx: &Transaction,
        _tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        Ok(TxAcceptAction::Accept {
            conflicts_with: HashSet::new(),
        })
    }
}

impl<C0, C1> CusfEnforcer for Either<C0, C1>
where
    C0: CusfEnforcer + Send,
    C1: CusfEnforcer + Send,
{
    type SyncError = Either<C0::SyncError, C1::SyncError>;

    async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        tip: BlockHash,
    ) -> Result<(), Self::SyncError> {
        let shutdown_signal = shutdown_signal.shared();
        match self {
            Self::Left(left) => {
                left.sync_to_tip(shutdown_signal, tip)
                    .map_err(Either::Left)
                    .await
            }
            Self::Right(right) => {
                right
                    .sync_to_tip(shutdown_signal, tip)
                    .map_err(Either::Right)
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

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        match self {
            Self::Left(left) => {
                left.accept_tx(tx, tx_inputs).map_err(Either::Left)
            }
            Self::Right(right) => {
                right.accept_tx(tx, tx_inputs).map_err(Either::Right)
            }
        }
    }
}
