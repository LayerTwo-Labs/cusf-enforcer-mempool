use std::{convert::Infallible, fmt::Debug, future::Future};

use bitcoin::{BlockHash, TxOut};
use either::Either;

use crate::cusf_enforcer::{self, CusfEnforcer};

/// re-exported to use as a constraint when implementing [`CusfBlockProducer`]
pub use typewit;

mod private {
    pub trait Sealed {}
}

impl<const B: bool> private::Sealed for typewit::const_marker::Bool<B> {}

pub trait CoinbaseTxn: private::Sealed {
    type CoinbaseTxouts: Clone + Debug + Default + Send + Sync;
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

pub mod initial_block_template {
    use std::collections::HashSet;

    use bitcoin::{Transaction, Txid};

    use crate::cusf_block_producer::CoinbaseTxn;

    pub type TxsItem = (Transaction, bitcoin::Amount);

    #[derive(Clone, Debug)]
    pub enum SuffixTxsItem {
        /// Transaction is already generated
        Tx(TxsItem),
        /// Reserved, tx will be generated after mempool txs are selected
        Reserved { weight: bitcoin::Weight },
    }

    impl SuffixTxsItem {
        pub fn weight(&self) -> bitcoin::Weight {
            match self {
                Self::Tx((tx, _)) => tx.weight(),
                Self::Reserved { weight } => *weight,
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct Template<const COINBASE_TXN: bool>
    where typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn
    {
        pub coinbase_txouts: <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts,
        /// Prefix txs, with absolute fee
        pub prefix_txs: Vec<TxsItem>,
        /// Suffix txs
        pub suffix_txs: Vec<SuffixTxsItem>,
        /// prefix/suffix txs do not need to be included here
        pub exclude_mempool_txs: HashSet<Txid>,
    }
}
pub use initial_block_template::Template as InitialBlockTemplate;

pub struct FilledBlockTemplateMut<'a, const COINBASE_TXN: bool>
    where typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn
{
    pub coinbase_txouts: &'a mut <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts,
    /// Prefix txs, with absolute fee
    pub prefix_txs: &'a Vec<initial_block_template::TxsItem>,
    /// Suffix txs
    pub suffix_txs: &'a mut Vec<initial_block_template::TxsItem>,
}

#[derive(educe::Educe)]
#[educe(Clone, Debug, Default)]
pub struct FilledBlockTemplate<const COINBASE_TXN: bool>
where typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn
{
    pub(crate) coinbase_txouts: <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts,
    /// Prefix txs, with absolute fee
    pub(crate) prefix_txs: Vec<initial_block_template::TxsItem>,
    /// Suffix txs
    pub(crate) suffix_txs: Vec<initial_block_template::TxsItem>,
}

impl<const COINBASE_TXN: bool> FilledBlockTemplate<COINBASE_TXN>
where
    typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
{
    pub fn as_mut(&mut self) -> FilledBlockTemplateMut<'_, COINBASE_TXN> {
        let Self {
            coinbase_txouts,
            prefix_txs,
            suffix_txs,
        } = self;
        FilledBlockTemplateMut {
            coinbase_txouts,
            prefix_txs,
            suffix_txs,
        }
    }

    pub fn coinbase_txouts(&mut self) -> &mut <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts{
        &mut self.coinbase_txouts
    }

    /// Prefix txs, with absolute fee
    pub fn prefix_txs(&self) -> &Vec<initial_block_template::TxsItem> {
        &self.prefix_txs
    }

    /// Suffix txs
    pub fn suffix_txs(&mut self) -> &mut Vec<initial_block_template::TxsItem> {
        &mut self.suffix_txs
    }
}

impl<const COINBASE_TXN: bool> From<InitialBlockTemplate<COINBASE_TXN>>
    for FilledBlockTemplate<COINBASE_TXN>
where
    typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
{
    fn from(template: InitialBlockTemplate<COINBASE_TXN>) -> Self {
        let InitialBlockTemplate {
            coinbase_txouts,
            prefix_txs,
            suffix_txs,
            exclude_mempool_txs: _,
        } = template;
        let suffix_txs = suffix_txs
            .into_iter()
            .filter_map(|suffix_txs_item| match suffix_txs_item {
                initial_block_template::SuffixTxsItem::Tx(tx_item) => {
                    Some(tx_item)
                }
                initial_block_template::SuffixTxsItem::Reserved { .. } => None,
            })
            .collect();
        Self {
            coinbase_txouts,
            prefix_txs,
            suffix_txs,
        }
    }
}

pub trait CusfBlockProducer: CusfEnforcer {
    type InitialBlockTemplateError: std::error::Error + Send + Sync + 'static;

    fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut InitialBlockTemplate<COINBASE_TXN>,
    ) -> impl Future<Output = Result<(), Self::InitialBlockTemplateError>> + Send
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn;

    type FinalizeBlockTemplateError: std::error::Error + Send + Sync + 'static;

    /// Finalize coinbase outputs / suffix txs for a block template, after
    /// filling with proposed txs
    fn finalize_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut FilledBlockTemplate<COINBASE_TXN>,
    ) -> impl Future<Output = Result<(), Self::FinalizeBlockTemplateError>> + Send
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn;
}

impl<C0, C1> CusfBlockProducer for cusf_enforcer::Compose<C0, C1>
where
    C0: CusfBlockProducer + Send + Sync + 'static,
    C1: CusfBlockProducer + Send + Sync + 'static,
{
    type InitialBlockTemplateError =
        Either<C0::InitialBlockTemplateError, C1::InitialBlockTemplateError>;

    async fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::InitialBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        self.0
            .initial_block_template(
                parent_block_hash,
                coinbase_txn_wit,
                template,
            )
            .await
            .map_err(Either::Left)?;
        self.1
            .initial_block_template(
                parent_block_hash,
                coinbase_txn_wit,
                template,
            )
            .await
            .map_err(Either::Right)
    }

    type FinalizeBlockTemplateError =
        Either<C0::FinalizeBlockTemplateError, C1::FinalizeBlockTemplateError>;

    async fn finalize_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut FilledBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::FinalizeBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        let () = self
            .0
            .finalize_block_template(
                parent_block_hash,
                coinbase_txn_wit,
                template,
            )
            .await
            .map_err(Either::Left)?;
        let () = self
            .1
            .finalize_block_template(
                parent_block_hash,
                coinbase_txn_wit,
                template,
            )
            .await
            .map_err(Either::Right)?;
        Ok(())
    }
}

impl CusfBlockProducer for cusf_enforcer::DefaultEnforcer {
    type InitialBlockTemplateError = Infallible;

    async fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        _parent_block_hash: &BlockHash,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        _template: &mut InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::InitialBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        Ok(())
    }

    type FinalizeBlockTemplateError = Infallible;

    async fn finalize_block_template<const COINBASE_TXN: bool>(
        &self,
        _parent_block_hash: &BlockHash,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        _template: &mut FilledBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::FinalizeBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        Ok(())
    }
}

impl<C0, C1> CusfBlockProducer for Either<C0, C1>
where
    C0: CusfBlockProducer + Send + Sync,
    C1: CusfBlockProducer + Send + Sync,
{
    type InitialBlockTemplateError =
        Either<C0::InitialBlockTemplateError, C1::InitialBlockTemplateError>;

    async fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::InitialBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        match self {
            Self::Left(left) => left
                .initial_block_template(
                    parent_block_hash,
                    coinbase_txn_wit,
                    template,
                )
                .await
                .map_err(Either::Left),
            Self::Right(right) => right
                .initial_block_template(
                    parent_block_hash,
                    coinbase_txn_wit,
                    template,
                )
                .await
                .map_err(Either::Right),
        }
    }

    type FinalizeBlockTemplateError =
        Either<C0::FinalizeBlockTemplateError, C1::FinalizeBlockTemplateError>;

    async fn finalize_block_template<const COINBASE_TXN: bool>(
        &self,
        parent_block_hash: &BlockHash,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &mut FilledBlockTemplate<COINBASE_TXN>,
    ) -> Result<(), Self::FinalizeBlockTemplateError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        match self {
            Self::Left(left) => left
                .finalize_block_template(
                    parent_block_hash,
                    coinbase_txn_wit,
                    template,
                )
                .await
                .map_err(Either::Left),
            Self::Right(right) => right
                .finalize_block_template(
                    parent_block_hash,
                    coinbase_txn_wit,
                    template,
                )
                .await
                .map_err(Either::Right),
        }
    }
}
