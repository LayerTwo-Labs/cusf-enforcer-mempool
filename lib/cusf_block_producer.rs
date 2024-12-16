use std::{collections::HashSet, convert::Infallible, fmt::Debug};

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

pub trait CusfBlockProducer: CusfEnforcer {
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

impl<C0, C1> CusfBlockProducer for cusf_enforcer::Compose<C0, C1>
where
    C0: CusfBlockProducer + Send + 'static,
    C1: CusfBlockProducer + Send + 'static,
{
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

impl CusfBlockProducer for cusf_enforcer::DefaultEnforcer {
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

impl<C0, C1> CusfBlockProducer for Either<C0, C1>
where
    C0: CusfBlockProducer + Send,
    C1: CusfBlockProducer + Send,
{
    type InitialBlockTemplateError =
        Either<C0::InitialBlockTemplateError, C1::InitialBlockTemplateError>;

    fn initial_block_template<const COINBASE_TXN: bool>(
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
                .map_err(Either::Left),
            Self::Right(right) => right
                .initial_block_template(coinbase_txn_wit, template)
                .map_err(Either::Right),
        }
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
        match self {
            Self::Left(left) => left
                .suffix_txs(coinbase_txn_wit, template)
                .map_err(Either::Left),
            Self::Right(right) => right
                .suffix_txs(coinbase_txn_wit, template)
                .map_err(Either::Right),
        }
    }
}
