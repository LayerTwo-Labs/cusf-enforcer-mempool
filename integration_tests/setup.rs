//! Test fixtures: spawn bitcoind, set up a wallet, then (optionally) run
//! `init_sync_mempool` + start a `MempoolSync` against a `MockEnforcer`.

use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use bitcoin::{Address, BlockHash, Network, Txid};
use cusf_enforcer_mempool::{
    cusf_enforcer::CusfEnforcer,
    mempool::{self, MempoolSync, SyncTaskError},
};
use parking_lot::Mutex;
use reserve_port::ReservedPort;
use temp_dir::TempDir;
use tokio::time::sleep;

use crate::{
    mock_enforcer::MockEnforcer,
    util::{
        self, BinPaths, Bitcoind, RpcClient, create_wallet, get_new_address,
        wait_for_bitcoind_ready,
    },
};

/// Per-test directory layout: a leaked temp dir with a `bitcoind/` subdir
/// (where the bitcoind process writes its data and `stdout.txt`/`stderr.txt`).
/// Leaked so logs survive after the test ends — helpful for debugging and
/// for the end-of-run failure summary.
#[derive(Clone, Debug)]
pub struct Directories {
    pub base_dir: PathBuf,
    pub bitcoind_dir: PathBuf,
}

impl Directories {
    pub fn new() -> anyhow::Result<Self> {
        let temp = TempDir::new().context("creating temp dir")?;
        let base_dir = temp.path().to_path_buf();
        let bitcoind_dir = base_dir.join("bitcoind");
        std::fs::create_dir(&bitcoind_dir)?;
        temp.leak();
        Ok(Self {
            base_dir,
            bitcoind_dir,
        })
    }
}

#[derive(Debug)]
pub struct ReservedPorts {
    pub bitcoind_listen: ReservedPort,
    pub bitcoind_rpc: ReservedPort,
    pub bitcoind_zmq_sequence: ReservedPort,
}

impl ReservedPorts {
    pub fn new() -> Result<Self, reserve_port::Error> {
        Ok(Self {
            bitcoind_listen: ReservedPort::random()?,
            bitcoind_rpc: ReservedPort::random()?,
            bitcoind_zmq_sequence: ReservedPort::random()?,
        })
    }
}

/// Errors surfaced by the running `MempoolSync` task.
#[derive(Clone, Debug, Default)]
pub struct TaskErrors {
    inner: Arc<Mutex<Vec<String>>>,
}

impl TaskErrors {
    pub fn snapshot(&self) -> Vec<String> {
        self.inner.lock().clone()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// Bitcoind spawned with a funded regtest wallet and 110 priming blocks.
/// Tests that need to seed bitcoind state between bitcoind-spawn and
/// init-sync construct this directly, do their seeding inline against
/// `rpc_client`, and then call [`TestSetup::start`].
pub struct RegtestNode {
    pub bitcoind: Bitcoind,
    /// JSON-RPC client scoped to `it-wallet`.
    pub rpc_client: RpcClient,
    pub mining_address: Address,
    pub directories: Directories,
    // Drop order: bitcoind process → ports.
    _bitcoind_handle: util::AbortOnDrop<()>,
    _reserved_ports: ReservedPorts,
}

impl RegtestNode {
    pub const WALLET: &'static str = "it-wallet";

    pub async fn new(
        bin_paths: &BinPaths,
        directories: Directories,
    ) -> anyhow::Result<Self> {
        let reserved_ports = ReservedPorts::new()?;
        let bitcoind = Bitcoind {
            path: bin_paths.bitcoind()?.to_path_buf(),
            data_dir: directories.bitcoind_dir.clone(),
            listen_port: reserved_ports.bitcoind_listen.port(),
            rpc_port: reserved_ports.bitcoind_rpc.port(),
            rpc_user: "regtest".to_owned(),
            rpc_pass: "regtest".to_owned(),
            zmq_sequence_port: reserved_ports.bitcoind_zmq_sequence.port(),
        };
        tracing::info!(
            rpc_port = bitcoind.rpc_port,
            zmq_port = bitcoind.zmq_sequence_port,
            data_dir = %bitcoind.data_dir.display(),
            "spawning bitcoind"
        );
        let bitcoind_handle = bitcoind.spawn().context("spawning bitcoind")?;

        let rpc_client = {
            let daemon_rpc = bitcoind.http_client(None)?;
            wait_for_bitcoind_ready(&daemon_rpc).await?;
            create_wallet(&daemon_rpc, Self::WALLET)
                .await
                .context("createwallet")?;

            bitcoind.http_client(Some(Self::WALLET))?
        };

        let mining_address: Address = get_new_address(&rpc_client)
            .await
            .context("getnewaddress")?;

        // 110 (not 101) so the wallet keeps several mature coinbases — a
        // reorg that strips the latest coinbase otherwise hits "Insufficient
        // funds" in tests that bump-fee or send another tx.
        bitcoin_jsonrpsee::MainClient::generate_to_address(
            &rpc_client,
            110,
            &mining_address.clone().into_unchecked(),
        )
        .await
        .context("generatetoaddress 110")?;

        Ok(Self {
            bitcoind,
            rpc_client,
            mining_address,
            directories,
            _bitcoind_handle: bitcoind_handle,
            _reserved_ports: reserved_ports,
        })
    }
}

/// Wire `init_sync_mempool` + `MempoolSync::new` against `enforcer`.
/// Generic over any `CusfEnforcer + Clone + Send + Sync + 'static`.
pub async fn start_mempool_sync<E>(
    node: &RegtestNode,
    mut enforcer: E,
) -> anyhow::Result<(MempoolSync<E>, TaskErrors)>
where
    E: CusfEnforcer + Clone + Send + Sync + 'static,
{
    let (sequence_stream, mempool, tx_cache) = mempool::init_sync_mempool(
        &mut enforcer,
        Network::Regtest,
        &node.rpc_client,
        &node.bitcoind.zmq_addr(),
        std::future::pending::<()>(),
    )
    .await
    .context("init_sync_mempool")?;

    let task_errors = TaskErrors::default();
    let errors_for_handler = task_errors.clone();
    let mempool_sync = MempoolSync::new(
        enforcer,
        mempool,
        tx_cache,
        node.rpc_client.clone(),
        sequence_stream,
        move |err: SyncTaskError<E>| async move {
            let msg = format!("{err:#}");
            tracing::error!(err = %msg, "MempoolSync task error");
            errors_for_handler.inner.lock().push(msg);
        },
    );
    Ok((mempool_sync, task_errors))
}

/// `RegtestNode` + a running `MempoolSync` over a shared `MockEnforcer`. The
/// methods on `TestSetup` are limited to the non-trivial mempool-sync
/// observations; everything bitcoind-related goes through
/// `setup.node.rpc_client` (or the `util::*` RPC helpers for wallet RPCs
/// not exposed by `MainClient`).
pub struct TestSetup {
    pub node: RegtestNode,
    pub mempool_sync: MempoolSync<MockEnforcer>,
    pub enforcer: MockEnforcer,
    pub task_errors: TaskErrors,
}

impl TestSetup {
    /// One-shot: spawn bitcoind, mine priming blocks, run init-sync, start
    /// the task. For tests that need to seed bitcoind between spawn and
    /// init-sync, build a [`RegtestNode`] directly and call
    /// [`TestSetup::start`].
    pub async fn new(
        bin_paths: &BinPaths,
        directories: Directories,
    ) -> anyhow::Result<Self> {
        Self::start(RegtestNode::new(bin_paths, directories).await?).await
    }

    /// Run `init_sync_mempool` + start the task on an existing `RegtestNode`.
    pub async fn start(node: RegtestNode) -> anyhow::Result<Self> {
        let enforcer = MockEnforcer::new();
        let (mempool_sync, task_errors) =
            start_mempool_sync(&node, enforcer.clone()).await?;
        Ok(Self {
            node,
            mempool_sync,
            enforcer,
            task_errors,
        })
    }

    /// Submit a tx and wait for it to appear in the local `MempoolSync`
    /// view. The most-common composite operation.
    pub async fn submit_and_wait(
        &self,
        amount_sat: u64,
    ) -> anyhow::Result<Txid> {
        let txid = util::submit_tx(&self.node.rpc_client, amount_sat).await?;
        self.wait_for_local_mempool(
            Duration::from_secs(5),
            |t| t.contains(&txid),
            "submitted tx",
        )
        .await?;
        Ok(txid)
    }

    pub async fn local_mempool_txids(&self) -> HashSet<Txid> {
        local_mempool_txids(&self.mempool_sync).await
    }

    pub async fn wait_for_local_mempool<F>(
        &self,
        timeout_duration: Duration,
        predicate: F,
        label: &str,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&HashSet<Txid>) -> bool,
    {
        wait_for_local_mempool(
            &self.mempool_sync,
            &self.task_errors,
            timeout_duration,
            predicate,
            label,
        )
        .await
    }

    pub async fn wait_for_local_tip(
        &self,
        expected: BlockHash,
        timeout_duration: Duration,
    ) -> anyhow::Result<()> {
        wait_for_local_tip(
            &self.mempool_sync,
            &self.task_errors,
            expected,
            timeout_duration,
        )
        .await
    }
}

// --- Free fns over `&MempoolSync<E>` (used by tests with non-Mock enforcers). ---

pub async fn local_mempool_txids<E>(sync: &MempoolSync<E>) -> HashSet<Txid>
where
    E: CusfEnforcer + Send + Sync + 'static,
{
    sync.with(|mempool, _| {
        Box::pin(async move {
            mempool
                .propose_txs(None)
                .map(|v| v.into_iter().map(|t| t.txid).collect())
                .unwrap_or_default()
        })
    })
    .await
    .unwrap_or_default()
}

pub async fn wait_for_local_mempool<E, F>(
    sync: &MempoolSync<E>,
    task_errors: &TaskErrors,
    timeout_duration: Duration,
    mut predicate: F,
    label: &str,
) -> anyhow::Result<()>
where
    E: CusfEnforcer + Send + Sync + 'static,
    F: FnMut(&HashSet<Txid>) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut last = HashSet::new();
    while tokio::time::Instant::now() < deadline {
        last = local_mempool_txids(sync).await;
        if predicate(&last) {
            return Ok(());
        }
        if !task_errors.is_empty() {
            anyhow::bail!(
                "MempoolSync task surfaced errors while waiting for `{label}`: {:?}",
                task_errors.snapshot()
            );
        }
        sleep(Duration::from_millis(75)).await;
    }
    anyhow::bail!(
        "timed out waiting for `{label}` after {timeout_duration:?}; \
         last local mempool: {last:?}; task errors: {:?}",
        task_errors.snapshot()
    );
}

pub async fn wait_for_local_tip<E>(
    sync: &MempoolSync<E>,
    task_errors: &TaskErrors,
    expected: BlockHash,
    timeout_duration: Duration,
) -> anyhow::Result<()>
where
    E: CusfEnforcer + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let current = sync
            .with(|mempool, _| Box::pin(async move { mempool.tip().hash }))
            .await;
        if current == Some(expected) {
            return Ok(());
        }
        if !task_errors.is_empty() {
            anyhow::bail!(
                "MempoolSync task surfaced errors while waiting for tip {expected}: {:?}",
                task_errors.snapshot()
            );
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local tip = {expected} after \
                 {timeout_duration:?}; saw {current:?}; task errors: {:?}",
                task_errors.snapshot()
            );
        }
        sleep(Duration::from_millis(75)).await;
    }
}
