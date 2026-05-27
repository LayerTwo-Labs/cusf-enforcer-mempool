//! A simple, configurable mock [`CusfEnforcer`] for integration tests.
//! Shared with the sync task via an internal `Arc<Mutex<_>>`, so tests can
//! tweak policy from outside while the task is running.

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::Future,
    sync::Arc,
};

use bitcoin::{BlockHash, Transaction, Txid};
use cusf_enforcer_mempool::cusf_enforcer::{
    ConnectBlockAction, CusfEnforcer, DisconnectBlockAction, TxAcceptAction,
};
use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub enum MockCall {
    SyncToTip(BlockHash),
    ConnectBlock(BlockHash),
    DisconnectBlock(BlockHash),
    AcceptTx(Txid),
}

#[derive(Default)]
struct MockEnforcerInner {
    reject_txids: HashSet<Txid>,
    reject_all: bool,
    reject_blocks: HashSet<BlockHash>,
    reject_all_blocks: bool,
    remove_on_connect: HashMap<BlockHash, HashSet<Txid>>,
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

    pub fn set_reject_all(&self, reject_all: bool) {
        self.inner.lock().reject_all = reject_all;
    }

    pub fn set_reject_all_blocks(&self, reject_all: bool) {
        self.inner.lock().reject_all_blocks = reject_all;
    }

    pub fn set_always_remove_on_disconnect(&self, txids: HashSet<Txid>) {
        self.inner.lock().always_remove_on_disconnect = txids;
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
}

impl CusfEnforcer for MockEnforcer {
    type SyncError = Infallible;

    fn sync_to_tip<Signal: Future<Output = ()> + Send>(
        &mut self,
        _shutdown_signal: Signal,
        tip: BlockHash,
    ) -> impl Future<Output = Result<(), Self::SyncError>> + Send {
        let inner = self.inner.clone();
        async move {
            inner.lock().log.push(MockCall::SyncToTip(tip));
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
            let remove_mempool_txs = inner
                .remove_on_connect
                .remove(&block_hash)
                .unwrap_or_default();
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

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        _tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<TxAcceptAction, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
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
}
