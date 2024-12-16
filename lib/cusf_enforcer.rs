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
use futures::{TryFutureExt, TryStreamExt};
use thiserror::Error;

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

pub trait CusfEnforcer {
    type SyncError: std::error::Error + Send + Sync + 'static;

    /// Attempt to sync to the specified tip
    fn sync_to_tip(
        &mut self,
        tip: BlockHash,
    ) -> impl Future<Output = Result<(), Self::SyncError>> + Send;

    type ConnectBlockError: std::error::Error + Send + Sync + 'static;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError>;

    type DisconnectBlockError: std::error::Error + Send + Sync + 'static;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<(), Self::DisconnectBlockError>;

    type AcceptTxError: std::error::Error + Send + Sync + 'static;

    /// Return `true` to accept the tx, or `false` to reject it.
    /// Inputs to a tx are always available.
    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
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
    JsonRpc(#[from] bip300301::jsonrpsee::core::ClientError),
    #[error(transparent)]
    SequenceStream(#[from] crate::zmq::SequenceStreamError),
    #[error("ZMQ sequence stream ended unexpectedly")]
    SequenceStreamEnded,
    #[error(transparent)]
    Zmq(#[from] zeromq::ZmqError),
}

/// Subscribe to ZMQ sequence and sync enforcer, obtaining a ZMQ sequence
/// stream and best block hash
// 0. Subscribe to ZMQ sequence
// 1. Get best block hash
// 2. Sync enforcer to best block hash.
// 3. Get best block hash
// 4. If best block hash has changed, drop messages up to and including
//    (dis)connecting to best block hash, and go to step 2.
pub async fn initial_sync<'a, Enforcer, MainClient>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    zmq_addr_sequence: &str,
) -> Result<
    (BlockHash, crate::zmq::SequenceStream<'a>),
    InitialSyncError<Enforcer>,
>
where
    Enforcer: CusfEnforcer,
    MainClient: bip300301::client::MainClient + Sync,
{
    let mut sequence_stream =
        crate::zmq::subscribe_sequence(zmq_addr_sequence).await?;
    let mut block_hash = main_client.getbestblockhash().await?;
    let mut block_parent =
        main_client.getblockheader(block_hash).await?.prev_blockhash;
    'sync: loop {
        let () = enforcer
            .sync_to_tip(block_hash)
            .map_err(InitialSyncError::CusfEnforcer)
            .await?;
        let best_block_hash = main_client.getbestblockhash().await?;
        if block_hash == best_block_hash {
            return Ok((block_hash, sequence_stream));
        } else {
            'drop_seq_msgs: loop {
                let Some(msg) = sequence_stream.try_next().await? else {
                    return Err(InitialSyncError::SequenceStreamEnded);
                };
                match msg {
                    crate::zmq::SequenceMessage::BlockHash(block_hash_msg) => {
                        match block_hash_msg.event {
                            crate::zmq::BlockHashEvent::Connected => {
                                block_parent = block_hash;
                                block_hash = block_hash_msg.block_hash;
                            }
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
                    crate::zmq::SequenceMessage::TxHash(_) => {
                        continue 'drop_seq_msgs;
                    }
                }
            }
            continue 'sync;
        }
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
    JsonRpc(#[from] bip300301::jsonrpsee::core::ClientError),
    #[error(transparent)]
    ZmqSequence(#[from] crate::zmq::SequenceStreamError),
    #[error("ZMQ sequence stream ended unexpectedly")]
    ZmqSequenceEnded,
}

/// Run an enforcer in sync with a node
pub async fn task<'a, Enforcer, MainClient>(
    enforcer: &mut Enforcer,
    main_client: &MainClient,
    zmq_addr_sequence: &str,
) -> Result<Infallible, TaskError<Enforcer>>
where
    Enforcer: CusfEnforcer,
    MainClient: bip300301::client::MainClient + Sync,
{
    use crate::zmq::{BlockHashEvent, BlockHashMessage, SequenceMessage};
    use bip300301::client::{GetBlockClient as _, U8Witness};
    let (_best_block_hash, mut sequence_stream) =
        initial_sync(enforcer, main_client, zmq_addr_sequence).await?;
    while let Some(sequence_msg) = sequence_stream.try_next().await? {
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
                    .map_err(TaskError::ConnectBlock)?
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
                let () = enforcer
                    .disconnect_block(block_hash)
                    .map_err(TaskError::DisconnectBlock)?;
            }
        }
    }
    Err(TaskError::ZmqSequenceEnded)
}

/// Run an enforcer in sync with a node
pub fn spawn_task<Enforcer, MainClient, ErrHandler, ErrHandlerFut>(
    mut enforcer: Enforcer,
    main_client: MainClient,
    zmq_addr_sequence: String,
    err_handler: ErrHandler,
) -> tokio::task::JoinHandle<()>
where
    Enforcer: CusfEnforcer + Send + 'static,
    MainClient: bip300301::client::MainClient + Send + Sync + 'static,
    ErrHandler: FnOnce(TaskError<Enforcer>) -> ErrHandlerFut + Send + 'static,
    ErrHandlerFut: Future<Output = ()> + Send,
{
    tokio::task::spawn(async move {
        let Err(err) =
            task(&mut enforcer, &main_client, &zmq_addr_sequence).await;
        err_handler(err).await
    })
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

    async fn sync_to_tip(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<(), Self::SyncError> {
        let () = self.0.sync_to_tip(block_hash).map_err(Either::Left).await?;
        self.1.sync_to_tip(block_hash).map_err(Either::Right).await
    }

    type ConnectBlockError = ComposeConnectBlockError<C0, C1>;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        let res_left = self.0.connect_block(block).map_err(|err| {
            Self::ConnectBlockError::ConnectBlock(Either::Left(err))
        })?;
        let res_right = self.1.connect_block(block).map_err(|err| {
            Self::ConnectBlockError::ConnectBlock(Either::Right(err))
        })?;
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
                let () = self.1.disconnect_block(block.block_hash()).map_err(
                    |err| {
                        Self::ConnectBlockError::DisconnectBlock(Either::Right(
                            err,
                        ))
                    },
                )?;
                Ok(ConnectBlockAction::Reject)
            }
            (
                ConnectBlockAction::Accept {
                    remove_mempool_txs: _,
                },
                ConnectBlockAction::Reject,
            ) => {
                // Disconnect block on left enforcer
                let () = self.0.disconnect_block(block.block_hash()).map_err(
                    |err| {
                        Self::ConnectBlockError::DisconnectBlock(Either::Left(
                            err,
                        ))
                    },
                )?;
                Ok(ConnectBlockAction::Reject)
            }
            (ConnectBlockAction::Reject, ConnectBlockAction::Reject) => {
                Ok(ConnectBlockAction::Reject)
            }
        }
    }

    type DisconnectBlockError =
        Either<C0::DisconnectBlockError, C1::DisconnectBlockError>;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<(), Self::DisconnectBlockError> {
        let () = self.0.disconnect_block(block_hash).map_err(Either::Left)?;
        self.1.disconnect_block(block_hash).map_err(Either::Right)
    }

    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        if self.0.accept_tx(tx, tx_inputs).map_err(Either::Left)? {
            self.1.accept_tx(tx, tx_inputs).map_err(Either::Right)
        } else {
            Ok(false)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultEnforcer;

impl CusfEnforcer for DefaultEnforcer {
    type SyncError = Infallible;

    async fn sync_to_tip(
        &mut self,
        _block_hash: BlockHash,
    ) -> Result<(), Self::SyncError> {
        Ok(())
    }

    type ConnectBlockError = Infallible;

    fn connect_block(
        &mut self,
        _block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        Ok(ConnectBlockAction::default())
    }

    type DisconnectBlockError = Infallible;

    fn disconnect_block(
        &mut self,
        _block_hash: BlockHash,
    ) -> Result<(), Self::DisconnectBlockError> {
        Ok(())
    }

    type AcceptTxError = Infallible;

    fn accept_tx<TxRef>(
        &mut self,
        _tx: &Transaction,
        _tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        Ok(true)
    }
}

impl<C0, C1> CusfEnforcer for Either<C0, C1>
where
    C0: CusfEnforcer + Send,
    C1: CusfEnforcer + Send,
{
    type SyncError = Either<C0::SyncError, C1::SyncError>;

    async fn sync_to_tip(
        &mut self,
        tip: BlockHash,
    ) -> Result<(), Self::SyncError> {
        match self {
            Self::Left(left) => {
                left.sync_to_tip(tip).map_err(Either::Left).await
            }
            Self::Right(right) => {
                right.sync_to_tip(tip).map_err(Either::Right).await
            }
        }
    }

    type ConnectBlockError =
        Either<C0::ConnectBlockError, C1::ConnectBlockError>;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> Result<ConnectBlockAction, Self::ConnectBlockError> {
        match self {
            Self::Left(left) => left.connect_block(block).map_err(Either::Left),
            Self::Right(right) => {
                right.connect_block(block).map_err(Either::Right)
            }
        }
    }

    type DisconnectBlockError =
        Either<C0::DisconnectBlockError, C1::DisconnectBlockError>;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<(), Self::DisconnectBlockError> {
        match self {
            Self::Left(left) => {
                left.disconnect_block(block_hash).map_err(Either::Left)
            }
            Self::Right(right) => {
                right.disconnect_block(block_hash).map_err(Either::Right)
            }
        }
    }

    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
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
