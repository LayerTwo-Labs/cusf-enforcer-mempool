//! An RPC client that can hold the sync task's next tx fetch open on demand.
//!
//! The point is to make fetch-vs-announcement ordering deterministic. Tests
//! that need "this tx is still being fetched when its removal arrives" would
//! otherwise have to race bitcoind and hope.

use std::sync::Arc;

use bitcoin_jsonrpsee::jsonrpsee::core::{
    ClientError,
    client::{BatchResponse, ClientT},
    params::BatchRequestBuilder,
    traits::ToRpcParams,
};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use tokio::sync::Notify;

use crate::util::RpcClient;

const TX_FETCH_METHOD: &str = "getrawtransaction";

/// One call of a stalled batch.
#[derive(Clone, Debug)]
pub struct BatchMember {
    pub method: String,
    /// The call's params, as raw JSON. Kept as a string rather than parsed:
    /// tests only ever ask whether a txid appears in it.
    pub params: String,
}

/// Wraps [`RpcClient`], holding the first tx fetch after [`Self::arm`] (or
/// [`Self::arm_batch`]) open until [`Self::release`].
///
/// Starts disarmed so init-sync completes normally. Arm it once the test is
/// ready for the stall.
#[derive(Clone)]
pub struct StallingClient {
    inner: RpcClient,
    armed: Arc<Mutex<bool>>,
    /// Separate arm for the batched fetch path, which goes through
    /// [`ClientT::batch_request`] rather than [`ClientT::request`].
    armed_batch: Arc<Mutex<bool>>,
    /// Method and params JSON of each member of the batch that
    /// [`Self::arm_batch`] stalled. Lets a test assert the request it parked
    /// really was a batched fetch *of the txs it cares about*, rather than
    /// pass because the sync task happened to issue singles, or batched some
    /// incidental other pair.
    stalled_batch: Arc<Mutex<Vec<BatchMember>>>,
    /// Notified once a stalled call is parked. `notify_one` rather than
    /// `notify_waiters` so the signal is not lost if the call is parked before
    /// the test starts waiting.
    arrived: Arc<Notify>,
    /// Notified by [`Self::release`]; the parked call waits on this.
    release: Arc<Notify>,
}

impl StallingClient {
    pub fn new(inner: RpcClient) -> Self {
        Self {
            inner,
            armed: Arc::new(Mutex::new(false)),
            armed_batch: Arc::new(Mutex::new(false)),
            stalled_batch: Arc::new(Mutex::new(Vec::new())),
            arrived: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    /// Stall the next single tx fetch.
    pub fn arm(&self) {
        *self.armed.lock() = true;
    }

    /// Stall the next *batched* request that contains a tx fetch.
    pub fn arm_batch(&self) {
        *self.armed_batch.lock() = true;
    }

    /// The batch stalled by [`Self::arm_batch`], empty until one is parked.
    pub fn stalled_batch(&self) -> Vec<BatchMember> {
        self.stalled_batch.lock().clone()
    }

    /// Wait until the stalled call is parked.
    pub async fn wait_until_stalled(&self) {
        self.arrived.notified().await
    }

    /// Let the parked call proceed.
    pub fn release(&self) {
        self.release.notify_waiters()
    }

    /// Consume the arm, if set. Only the first matching call stalls, so the
    /// sync task makes progress again once released.
    fn take_arm(&self) -> bool {
        let mut armed = self.armed.lock();
        std::mem::replace(&mut armed, false)
    }

    /// As [`Self::take_arm`], for the batch arm.
    fn take_batch_arm(&self) -> bool {
        let mut armed = self.armed_batch.lock();
        std::mem::replace(&mut armed, false)
    }
}

impl ClientT for StallingClient {
    fn notification<Params>(
        &self,
        method: &str,
        params: Params,
    ) -> impl Future<Output = Result<(), ClientError>> + Send
    where
        Params: ToRpcParams + Send,
    {
        self.inner.notification(method, params)
    }

    fn request<R, Params>(
        &self,
        method: &str,
        params: Params,
    ) -> impl Future<Output = Result<R, ClientError>> + Send
    where
        R: DeserializeOwned,
        Params: ToRpcParams + Send,
    {
        let should_stall = method == TX_FETCH_METHOD && self.take_arm();
        let arrived = self.arrived.clone();
        let release = self.release.clone();
        let inner_call = self.inner.request(method, params);
        async move {
            if should_stall {
                // Register interest BEFORE announcing arrival, or a `release`
                // landing between the two would be missed and this would hang.
                let notified = release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                arrived.notify_one();
                notified.await;
            }
            inner_call.await
        }
    }

    fn batch_request<'a, R>(
        &self,
        batch: BatchRequestBuilder<'a>,
    ) -> impl Future<Output = Result<BatchResponse<'a, R>, ClientError>> + Send
    where
        R: DeserializeOwned + std::fmt::Debug + 'a,
    {
        let members: Vec<BatchMember> = batch
            .iter()
            .map(|(method, params)| BatchMember {
                method: method.to_owned(),
                params: params.map(|p| p.get().to_owned()).unwrap_or_default(),
            })
            .collect();
        let should_stall = members.iter().any(|m| m.method == TX_FETCH_METHOD)
            && self.take_batch_arm();
        let arrived = self.arrived.clone();
        let release = self.release.clone();
        let stalled_batch = self.stalled_batch.clone();
        let inner_call = self.inner.batch_request(batch);
        async move {
            if should_stall {
                *stalled_batch.lock() = members;
                // Register interest BEFORE announcing arrival, as in
                // `request`.
                let notified = release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                arrived.notify_one();
                notified.await;
            }
            inner_call.await
        }
    }
}
