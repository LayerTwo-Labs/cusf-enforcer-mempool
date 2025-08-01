use std::{
    collections::HashSet, convert::Infallible, fmt::Debug, future::Future,
};

use bitcoin::{Transaction, TxOut, Txid};
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

#[derive(Clone, Debug, Default)]
pub struct BlockTemplateSuffix<const COINBASE_TXN: bool>
where typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn
{
    /// Suffix coinbase txouts
    pub coinbase_txouts: <typewit::const_marker::Bool<COINBASE_TXN> as CoinbaseTxn>::CoinbaseTxouts,
    /// Suffix txs, with absolute fee
    pub txs: Vec<(Transaction, bitcoin::Amount)>,
}

pub trait CusfBlockProducer: CusfEnforcer {
    type InitialBlockTemplateError: std::error::Error + Send + Sync + 'static;

    fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: InitialBlockTemplate<COINBASE_TXN>,
    ) -> impl Future<
        Output = Result<
            InitialBlockTemplate<COINBASE_TXN>,
            Self::InitialBlockTemplateError,
        >,
    > + Send
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn;

    type SuffixTxsError: std::error::Error + Send + Sync + 'static;

    /// Add outputs / txs to a block template, after filling with proposed txs
    fn block_template_suffix<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> impl Future<
        Output = Result<
            BlockTemplateSuffix<COINBASE_TXN>,
            Self::SuffixTxsError,
        >,
    > + Send
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
            .await
            .map_err(Either::Left)?;
        self.1
            .initial_block_template(coinbase_txn_wit, template)
            .await
            .map_err(Either::Right)
    }

    type SuffixTxsError = Either<C0::SuffixTxsError, C1::SuffixTxsError>;

    async fn block_template_suffix<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<BlockTemplateSuffix<COINBASE_TXN>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        let suffix_left = self
            .0
            .block_template_suffix(coinbase_txn_wit, template)
            .await
            .map_err(Either::Left)?;
        let mut template = template.clone();
        match coinbase_txn_wit {
            typewit::const_marker::BoolWit::True(wit) => {
                let wit = wit.map(CoinbaseTxouts);
                let coinbase_txouts: &mut Vec<_> =
                    wit.in_mut().to_right(&mut template.coinbase_txouts);
                coinbase_txouts.extend(
                    wit.in_ref()
                        .to_right(&suffix_left.coinbase_txouts)
                        .iter()
                        .cloned(),
                );
            }
            typewit::const_marker::BoolWit::False(_) => (),
        }
        template.prefix_txs.extend(suffix_left.txs.iter().cloned());
        let suffix_right = self
            .1
            .block_template_suffix(coinbase_txn_wit, &template)
            .await
            .map_err(Either::Right)?;
        let mut res = suffix_left;
        match coinbase_txn_wit {
            typewit::const_marker::BoolWit::True(wit) => {
                let wit = wit.map(CoinbaseTxouts);
                let coinbase_txouts: &mut Vec<_> =
                    wit.in_mut().to_right(&mut res.coinbase_txouts);
                coinbase_txouts
                    .extend(wit.to_right(suffix_right.coinbase_txouts));
            }
            typewit::const_marker::BoolWit::False(_) => (),
        }
        res.txs.extend(suffix_right.txs);
        Ok(res)
    }
}

impl CusfBlockProducer for cusf_enforcer::DefaultEnforcer {
    type InitialBlockTemplateError = Infallible;

    async fn initial_block_template<const COINBASE_TXN: bool>(
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

    async fn block_template_suffix<const COINBASE_TXN: bool>(
        &self,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        _template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<BlockTemplateSuffix<COINBASE_TXN>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        Ok(BlockTemplateSuffix::default())
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
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<
        InitialBlockTemplate<COINBASE_TXN>,
        Self::InitialBlockTemplateError,
    >
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        match self {
            Self::Left(left) => left
                .initial_block_template(coinbase_txn_wit, template)
                .await
                .map_err(Either::Left),
            Self::Right(right) => right
                .initial_block_template(coinbase_txn_wit, template)
                .await
                .map_err(Either::Right),
        }
    }

    type SuffixTxsError = Either<C0::SuffixTxsError, C1::SuffixTxsError>;

    async fn block_template_suffix<const COINBASE_TXN: bool>(
        &self,
        coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<BlockTemplateSuffix<COINBASE_TXN>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        match self {
            Self::Left(left) => left
                .block_template_suffix(coinbase_txn_wit, template)
                .await
                .map_err(Either::Left),
            Self::Right(right) => right
                .block_template_suffix(coinbase_txn_wit, template)
                .await
                .map_err(Either::Right),
        }
    }
}
