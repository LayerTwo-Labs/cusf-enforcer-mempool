use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt::Debug,
    future::Future,
};

use bitcoin::{BlockHash, Transaction, TxOut, Txid};
use educe::Educe;
use either::Either;
use futures::{TryFutureExt, TryStreamExt};
use thiserror::Error;

/// re-exported to use as a constraint when implementing [`CusfEnforcer`]
pub use typewit;

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

mod private {
    pub trait Sealed {}
}

impl<const B: bool> private::Sealed for typewit::const_marker::Bool<B> {}

pub trait CoinbaseTxn: private::Sealed {
    type CoinbaseTxouts: Clone + Debug + Default;
}

impl CoinbaseTxn for typewit::const_marker::Bool<true> {
    type CoinbaseTxouts = Vec<TxOut>;
}

impl CoinbaseTxn for typewit::const_marker::Bool<false> {
    type CoinbaseTxouts = ();
}

/// Marker struct to implement [`typewit::TypeFn`]
pub struct CoinbaseTxouts;

impl<const COINBASE_TXN: bool>
    typewit::TypeFn<typewit::const_marker::Bool<COINBASE_TXN>>
    for CoinbaseTxouts
where
    typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
{
    type Output = <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts;
}

#[derive(Clone, Debug, Default)]
pub struct InitialBlockTemplate<const COINBASE_TXN: bool>
where typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn
{
    pub coinbase_txouts: <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts,
    /// Prefix txs, with absolute fee
    pub prefix_txs: Vec<(Transaction, bitcoin::Amount)>,
    /// prefix txs do not need to be included here
    pub exclude_mempool_txs: HashSet<Txid>,
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

    type InitialBlockTemplateError: std::error::Error + Send + Sync + 'static;

    fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<
        InitialBlockTemplate<COINBASE_TXN>,
        Self::InitialBlockTemplateError,
    >
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn;

    type SuffixTxsError: std::error::Error + Send + Sync + 'static;

    /// Suffix txs, with absolute fee
    fn suffix_txs<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<Vec<(Transaction, bitcoin::Amount)>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn;
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
    #[error("CUSF Enforcer: error generating initial block template")]
    InitialBlockTemplate(#[source] Enforcer::InitialBlockTemplateError),
    #[error("CUSF Enforcer: error generating block suffix txs")]
    SuffixTxs(#[source] Enforcer::SuffixTxsError),
    #[error("CUSF Enforcer: error during initial sync")]
    Sync(#[source] Enforcer::SyncError),
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
pub struct Compose<C0, C1>(C0, C1);

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

    type InitialBlockTemplateError =
        Either<C0::InitialBlockTemplateError, C1::InitialBlockTemplateError>;

    fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        mut template: InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<
        InitialBlockTemplate<COINBASE_TXN>,
        Self::InitialBlockTemplateError,
    >
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        template = self
            .0
            .initial_block_template(coinbase_txn_wit, template)
            .map_err(Either::Left)?;
        self.1
            .initial_block_template(coinbase_txn_wit, template)
            .map_err(Either::Right)
    }

    type SuffixTxsError = Either<C0::SuffixTxsError, C1::SuffixTxsError>;

    fn suffix_txs<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<Vec<(Transaction, bitcoin::Amount)>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        let suffix_left = self
            .0
            .suffix_txs(coinbase_txn_wit, template)
            .map_err(Either::Left)?;
        let mut template = template.clone();
        template.prefix_txs.extend(suffix_left.iter().cloned());
        let suffix_right = self
            .1
            .suffix_txs(coinbase_txn_wit, &template)
            .map_err(Either::Right)?;
        let mut res = suffix_left;
        res.extend(suffix_right);
        Ok(res)
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

    type InitialBlockTemplateError = Infallible;

    fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<
        InitialBlockTemplate<COINBASE_TXN>,
        Self::InitialBlockTemplateError,
    >
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        Ok(template)
    }

    type SuffixTxsError = Infallible;

    fn suffix_txs<const COINBASE_TXN: bool>(
        &self,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        _template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<Vec<(Transaction, bitcoin::Amount)>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        Ok(Vec::new())
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
