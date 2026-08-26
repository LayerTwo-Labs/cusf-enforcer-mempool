//! A simple, configurable mock [`CusfEnforcer`] for integration tests.
//! Shared with the sync task via an internal `Arc<Mutex<_>>`, so tests can
//! tweak policy from outside while the task is running.

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::Future,
    sync::Arc,
};

use bitcoin::{BlockHash, Transaction, Txid};
use cusf_enforcer_mempool::{
    cusf_block_producer::{
        CoinbaseTxn, CusfBlockProducer, FilledBlockTemplate,
        InitialBlockTemplate, typewit,
    },
    cusf_enforcer::{
        ConnectBlockAction, CusfEnforcer, DisconnectBlockAction,
        SyncToTipError, TxAcceptAction,
    },
};
use parking_lot::Mutex;

/// The reason [`MockEnforcer::sync_to_tip`] reports a block as invalid
#[derive(Debug, thiserror::Error)]
#[error("mock enforcer: block is configured as invalid")]
pub struct MockInvalidBlockReason;

#[derive(Clone, Debug)]
pub enum MockCall {
    SyncToTip(BlockHash),
    ConnectBlock(BlockHash),
    DisconnectBlock(BlockHash),
    AcceptTx(Txid),
    ValidateBlock(BlockHash),
}

#[derive(Default)]
struct MockEnforcerInner {
    reject_txids: HashSet<Txid>,
    reject_all: bool,
    reject_blocks: HashSet<BlockHash>,
    reject_all_blocks: bool,
    /// Blocks that `sync_to_tip` reports as invalid, one per call, in order.
    /// Popped when reported: after the driver invalidates the reported block
    /// and retries, the retry no longer reports it, like a real enforcer
    /// syncing a chain that no longer contains the block.
    sync_invalid_blocks: Vec<BlockHash>,
    remove_on_connect: HashMap<BlockHash, HashSet<Txid>>,
    always_remove_on_connect: HashSet<Txid>,
    remove_on_disconnect: HashMap<BlockHash, HashSet<Txid>>,
    always_remove_on_disconnect: HashSet<Txid>,
    log: Vec<MockCall>,
}

#[derive(Clone, Default)]
pub struct MockEnforcer {
    inner: Arc<Mutex<MockEnforcerInner>>,
}

impl MockEnforcer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reject_tx(&self, txid: Txid) {
        self.inner.lock().reject_txids.insert(txid);
    }

    /// Make both `connect_block` and `validate_block` reject `block_hash`.
    pub fn reject_block(&self, block_hash: BlockHash) {
        self.inner.lock().reject_blocks.insert(block_hash);
    }

    pub fn set_reject_all(&self, reject_all: bool) {
        self.inner.lock().reject_all = reject_all;
    }

    pub fn set_reject_all_blocks(&self, reject_all: bool) {
        self.inner.lock().reject_all_blocks = reject_all;
    }

    /// Return `txids` in `remove_mempool_txs` for *every* connected block.
    /// Keyed on nothing rather than on a block hash because a test can only
    /// learn a block's hash after mining it, by which point the sync task may
    /// already have connected it.
    pub fn set_always_remove_on_connect(&self, txids: HashSet<Txid>) {
        self.inner.lock().always_remove_on_connect = txids;
    }

    pub fn set_always_remove_on_disconnect(&self, txids: HashSet<Txid>) {
        self.inner.lock().always_remove_on_disconnect = txids;
    }

    /// Make the next `sync_to_tip` call report `block_hash` as invalid.
    /// Multiple queued blocks are reported one per call, in order.
    pub fn report_invalid_block_on_sync(&self, block_hash: BlockHash) {
        self.inner.lock().sync_invalid_blocks.push(block_hash);
    }

    /// Tips that `sync_to_tip` has been invoked with, in call order.
    pub fn sync_to_tip_calls(&self) -> Vec<BlockHash> {
        self.inner
            .lock()
            .log
            .iter()
            .filter_map(|c| match c {
                MockCall::SyncToTip(tip) => Some(*tip),
                _ => None,
            })
            .collect()
    }

    pub fn disconnect_block_calls(&self) -> usize {
        self.inner
            .lock()
            .log
            .iter()
            .filter(|c| matches!(c, MockCall::DisconnectBlock(_)))
            .count()
    }

    /// Txids that `accept_tx` has been invoked with, in call order.
    pub fn accept_tx_calls(&self) -> Vec<Txid> {
        self.inner
            .lock()
            .log
            .iter()
            .filter_map(|c| match c {
                MockCall::AcceptTx(t) => Some(*t),
                _ => None,
            })
            .collect()
    }

    /// Blocks that `validate_block` has been invoked with, in call order.
    pub fn validate_block_calls(&self) -> Vec<BlockHash> {
        self.inner
            .lock()
            .log
            .iter()
            .filter_map(|c| match c {
                MockCall::ValidateBlock(block_hash) => Some(*block_hash),
                _ => None,
            })
            .collect()
    }
}

impl CusfEnforcer for MockEnforcer {
    type InvalidBlockReason = MockInvalidBlockReason;
    type SyncError = Infallible;

    fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        _shutdown_signal: Signal,
        tip: BlockHash,
    ) -> impl Future<
        Output = Result<
            (),
            SyncToTipError<Self::InvalidBlockReason, Self::SyncError>,
        >,
    > + Send {
        let inner = self.inner.clone();
        async move {
            let mut inner = inner.lock();
            inner.log.push(MockCall::SyncToTip(tip));
            if !inner.sync_invalid_blocks.is_empty() {
                let block_hash = inner.sync_invalid_blocks.remove(0);
                return Err(SyncToTipError::InvalidBlock {
                    block_hash,
                    reason: MockInvalidBlockReason,
                });
            }
            Ok(())
        }
    }

    type ConnectBlockError = Infallible;

    fn connect_block(
        &mut self,
        block: &bitcoin::Block,
    ) -> impl Future<
        Output = Result<ConnectBlockAction, Self::ConnectBlockError>,
    > + Send {
        let block_hash = block.block_hash();
        let inner = self.inner.clone();
        async move {
            let mut inner = inner.lock();
            inner.log.push(MockCall::ConnectBlock(block_hash));
            if inner.reject_all_blocks
                || inner.reject_blocks.contains(&block_hash)
            {
                return Ok(ConnectBlockAction::Reject);
            }
            let mut remove_mempool_txs = inner
                .remove_on_connect
                .remove(&block_hash)
                .unwrap_or_default();
            remove_mempool_txs
                .extend(inner.always_remove_on_connect.iter().copied());
            Ok(ConnectBlockAction::Accept { remove_mempool_txs })
        }
    }

    type DisconnectBlockError = Infallible;

    fn disconnect_block(
        &mut self,
        block_hash: BlockHash,
    ) -> impl Future<
        Output = Result<DisconnectBlockAction, Self::DisconnectBlockError>,
    > + Send {
        let inner = self.inner.clone();
        async move {
            let mut inner = inner.lock();
            inner.log.push(MockCall::DisconnectBlock(block_hash));
            let mut remove_mempool_txs = inner
                .remove_on_disconnect
                .remove(&block_hash)
                .unwrap_or_default();
            remove_mempool_txs
                .extend(inner.always_remove_on_disconnect.iter().copied());
            Ok(DisconnectBlockAction { remove_mempool_txs })
        }
    }

    type AcceptTxError = Infallible;

    fn accept_tx(
        &mut self,
        tx: &Transaction,
    ) -> Result<TxAcceptAction, Self::AcceptTxError> {
        let txid = tx.compute_txid();
        let mut inner = self.inner.lock();
        inner.log.push(MockCall::AcceptTx(txid));
        if inner.reject_all || inner.reject_txids.contains(&txid) {
            return Ok(TxAcceptAction::Reject);
        }
        Ok(TxAcceptAction::Accept {
            conflicts_with: HashSet::new(),
            weight_tweak: 0,
        })
    }

    type ValidateBlockError = Infallible;

    /// Mirrors `connect_block`'s policy, so a test can configure one rejection
    /// and observe it through either path.
    fn validate_block(
        &self,
        block: &bitcoin::Block,
    ) -> Result<Option<String>, Self::ValidateBlockError> {
        let block_hash = block.block_hash();
        let mut inner = self.inner.lock();
        inner.log.push(MockCall::ValidateBlock(block_hash));
        if inner.reject_all_blocks || inner.reject_blocks.contains(&block_hash)
        {
            return Ok(Some(
                "mock enforcer: block is configured as invalid".to_owned(),
            ));
        }
        Ok(None)
    }
}

/// The GBT server bounds its enforcer by `CusfBlockProducer`, so the mock
/// needs it to be usable there at all. Both hooks are deliberately no-ops:
/// the mock exists to exercise enforcer *policy*, and leaving the template
/// untouched matches what `DefaultEnforcer` does.
impl CusfBlockProducer for MockEnforcer {
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
