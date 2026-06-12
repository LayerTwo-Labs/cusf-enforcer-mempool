//! A mock [`CusfBlockProducer`] for integration tests: delegates enforcer
//! behavior to a [`MockEnforcer`], adds passthrough template hooks, and lets
//! tests declare proposal txs invalid from outside while the server is
//! running.

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::Future,
    sync::Arc,
};

use bitcoin::{BlockHash, Transaction, Txid};
use cusf_enforcer_mempool::{
    cusf_block_producer::{
        BlockTemplateSuffix, CoinbaseTxn, CusfBlockProducer,
        InitialBlockTemplate, ProposalValidity, typewit,
    },
    cusf_enforcer::{
        ConnectBlockAction, CusfEnforcer, DisconnectBlockAction, TxAcceptAction,
    },
};
use parking_lot::Mutex;

use crate::mock_enforcer::MockEnforcer;

#[derive(Default)]
struct MockBlockProducerInner {
    /// Proposals containing any of these txs are declared invalid due to
    /// that tx
    invalid_proposal_txids: HashSet<Txid>,
    /// If set, every proposal is declared invalid due to this txid,
    /// regardless of whether it is in the proposal
    force_invalid_txid: Option<Txid>,
    /// Proposal txids that `block_template_suffix` was invoked with, per
    /// call
    suffix_proposals: Vec<Vec<Txid>>,
}

#[derive(Clone, Default)]
pub struct MockBlockProducer {
    pub enforcer: MockEnforcer,
    inner: Arc<Mutex<MockBlockProducerInner>>,
}

impl MockBlockProducer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare proposals containing `txid` invalid due to it
    pub fn set_invalid_proposal_txid(&self, txid: Txid) {
        self.inner.lock().invalid_proposal_txids.insert(txid);
    }

    pub fn clear_invalid_proposal_txids(&self) {
        self.inner.lock().invalid_proposal_txids.clear();
    }

    /// Declare every proposal invalid due to `txid`, even if it is not in
    /// the proposal
    pub fn set_force_invalid_txid(&self, txid: Option<Txid>) {
        self.inner.lock().force_invalid_txid = txid;
    }

    /// Proposal txids that `block_template_suffix` was invoked with, per
    /// call
    pub fn suffix_proposals(&self) -> Vec<Vec<Txid>> {
        self.inner.lock().suffix_proposals.clone()
    }
}

impl CusfEnforcer for MockBlockProducer {
    type SyncError = <MockEnforcer as CusfEnforcer>::SyncError;

    fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        shutdown_signal: Signal,
        tip: BlockHash,
    ) -> impl Future<Output = Result<(), Self::SyncError>> + Send {
        self.enforcer.sync_to_tip(shutdown_signal, tip)
    }

    type ConnectBlockError = <MockEnforcer as CusfEnforcer>::ConnectBlockError;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> impl Future<
        Output = Result<ConnectBlockAction, Self::ConnectBlockError>,
    > + Send {
        self.enforcer.connect_block(block)
    }

    type DisconnectBlockError =
        <MockEnforcer as CusfEnforcer>::DisconnectBlockError;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> impl Future<
        Output = Result<DisconnectBlockAction, Self::DisconnectBlockError>,
    > + Send {
        self.enforcer.disconnect_block(block_hash)
    }

    type AcceptTxError = <MockEnforcer as CusfEnforcer>::AcceptTxError;

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        self.enforcer.accept_tx(tx, tx_inputs)
    }
}

impl CusfBlockProducer for MockBlockProducer {
    type InitialBlockTemplateError = Infallible;

    async fn initial_block_template<const COINBASE_TXN: bool>(
        &self,
        _parent_block_hash: &BlockHash,
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

    type ValidateBlockProposalError = Infallible;

    async fn validate_block_proposal<const COINBASE_TXN: bool>(
        &self,
        _parent_block_hash: &BlockHash,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<ProposalValidity, Self::ValidateBlockProposalError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        let inner = self.inner.lock();
        if let Some(forced) = inner.force_invalid_txid {
            return Ok(ProposalValidity::InvalidTx(forced));
        }
        for (tx, _fee) in &template.prefix_txs {
            let txid = tx.compute_txid();
            if inner.invalid_proposal_txids.contains(&txid) {
                return Ok(ProposalValidity::InvalidTx(txid));
            }
        }
        Ok(ProposalValidity::Valid)
    }

    type SuffixTxsError = Infallible;

    async fn block_template_suffix<const COINBASE_TXN: bool>(
        &self,
        _parent_block_hash: &BlockHash,
        _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
        template: &InitialBlockTemplate<COINBASE_TXN>,
    ) -> Result<BlockTemplateSuffix<COINBASE_TXN>, Self::SuffixTxsError>
    where
        typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
    {
        let proposal_txids = template
            .prefix_txs
            .iter()
            .map(|(tx, _fee)| tx.compute_txid())
            .collect();
        self.inner.lock().suffix_proposals.push(proposal_txids);
        Ok(BlockTemplateSuffix::default())
    }
}
