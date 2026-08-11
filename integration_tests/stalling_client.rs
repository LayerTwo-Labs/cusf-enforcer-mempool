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

/// Wraps [`RpcClient`], holding the first tx fetch after [`Self::arm`] open
/// until [`Self::release`].
///
/// Starts disarmed so init-sync completes normally. Arm it once the test is
/// ready for the stall.
#[derive(Clone)]
pub struct StallingClient {
    inner: RpcClient,
    armed: Arc<Mutex<bool>>,
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
            arrived: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    /// Stall the next tx fetch.
    pub fn arm(&self) {
        *self.armed.lock() = true;
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
        self.inner.batch_request(batch)
    }
}
