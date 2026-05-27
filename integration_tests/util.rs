use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use base64::Engine as _;
use bitcoin::{Address, BlockHash, Txid};
use bitcoin_jsonrpsee::{
    MainClient as _,
    jsonrpsee::{
        core::{ClientError, client::ClientT},
        http_client::{HeaderMap, HeaderValue, HttpClient, HttpClientBuilder},
        rpc_params,
    },
};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::{process::Command, task::JoinHandle, time::sleep};

/// JSON-RPC client targeting the test's bitcoind.
pub type RpcClient = HttpClient;

#[derive(Debug, Error)]
#[error("Missing or invalid environment variable `{key}`: {err}")]
pub struct VarError {
    key: String,
    err: std::env::VarError,
}

fn get_env_path(key: &str) -> Result<PathBuf, VarError> {
    std::env::var(key)
        .map(PathBuf::from)
        .map_err(|err| VarError {
            key: key.to_owned(),
            err,
        })
}

/// Lazily-resolved bin paths required by the integration tests.
#[derive(Clone, Debug, Default)]
pub struct BinPaths {
    bitcoind: std::sync::OnceLock<PathBuf>,
}

impl BinPaths {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bitcoind(&self) -> Result<&Path, VarError> {
        if let Some(p) = self.bitcoind.get() {
            return Ok(p);
        }
        let resolved = get_env_path("BITCOIND")?;
        Ok(self.bitcoind.get_or_init(|| resolved))
    }
}

/// Wrapper around `JoinHandle` that aborts the task on drop.
#[derive(Debug)]
#[repr(transparent)]
pub struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl<T> From<JoinHandle<T>> for AbortOnDrop<T> {
    fn from(task: JoinHandle<T>) -> Self {
        Self(task)
    }
}

/// Configuration for the `bitcoind` process spawned by an integration test.
#[derive(Clone, Debug)]
pub struct Bitcoind {
    pub path: PathBuf,
    pub data_dir: PathBuf,
    pub listen_port: u16,
    pub rpc_port: u16,
    pub rpc_user: String,
    pub rpc_pass: String,
    pub zmq_sequence_port: u16,
}

impl Bitcoind {
    pub fn spawn(&self) -> anyhow::Result<AbortOnDrop<()>> {
        let args: Vec<OsString> = [
            "-regtest".to_owned(),
            "-server".to_owned(),
            "-listenonion=0".to_owned(),
            "-fallbackfee=0.00001".to_owned(),
            "-acceptnonstdtxn=1".to_owned(),
            "-txindex".to_owned(),
            "-debug=mempool".to_owned(),
            "-debug=rpc".to_owned(),
            "-debug=zmq".to_owned(),
            format!("-datadir={}", self.data_dir.display()),
            format!("-bind=127.0.0.1:{}", self.listen_port),
            format!("-rpcbind=127.0.0.1:{}", self.rpc_port),
            "-rpcallowip=127.0.0.1".to_owned(),
            format!("-rpcuser={}", self.rpc_user),
            format!("-rpcpassword={}", self.rpc_pass),
            format!(
                "-zmqpubsequence=tcp://127.0.0.1:{}",
                self.zmq_sequence_port
            ),
        ]
        .into_iter()
        .map(OsString::from)
        .collect();

        let stdout_file =
            std::fs::File::create(self.data_dir.join("stdout.txt"))?;
        let stderr_file =
            std::fs::File::create(self.data_dir.join("stderr.txt"))?;

        let mut cmd = Command::new(&self.path);
        cmd.args(args);
        cmd.stdout(std::process::Stdio::from(stdout_file));
        cmd.stderr(std::process::Stdio::from(stderr_file));
        cmd.kill_on_drop(true);

        let mut child = cmd.spawn()?;
        let task = tokio::spawn(async move {
            // If bitcoind exits unexpectedly we just log; the test will fail
            // when it can't reach the RPC port. We don't want to panic from a
            // background task.
            match child.wait().await {
                Ok(status) => tracing::warn!(?status, "bitcoind exited"),
                Err(err) => tracing::warn!(%err, "bitcoind wait failed"),
            }
        });
        Ok(AbortOnDrop::from(task))
    }

    pub fn zmq_addr(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.zmq_sequence_port)
    }

    /// JSON-RPC client. If `wallet` is `Some`, the URL includes
    /// `/wallet/<name>` so wallet-only RPCs (sendtoaddress, bumpfee, etc.)
    /// are routed to that wallet.
    pub fn http_client(
        &self,
        wallet: Option<&str>,
    ) -> anyhow::Result<RpcClient> {
        // bitcoin_jsonrpsee::client builds the URL without a path; for
        // wallet-scoped calls we have to construct the builder ourselves.
        const MAX_REQUEST_SIZE: u32 = 100 * (1 << 20);
        const MAX_RESPONSE_SIZE: u32 = 1 << 30;
        const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

        let auth = format!("{}:{}", self.rpc_user, self.rpc_pass);
        let auth_value = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(auth)
        );
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_str(&auth_value)?);

        let url = match wallet {
            Some(name) => {
                format!("http://127.0.0.1:{}/wallet/{name}", self.rpc_port)
            }
            None => format!("http://127.0.0.1:{}", self.rpc_port),
        };

        Ok(HttpClientBuilder::default()
            .max_request_size(MAX_REQUEST_SIZE)
            .max_response_size(MAX_RESPONSE_SIZE)
            .request_timeout(REQUEST_TIMEOUT)
            .set_headers(headers)
            .build(url)?)
    }
}

/// Poll `getblockchaininfo` until bitcoind serves a successful response.
pub async fn wait_for_bitcoind_ready(rpc: &RpcClient) -> anyhow::Result<()> {
    const TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
    const POLL_INTERVAL: Duration = Duration::from_millis(200);
    let deadline = tokio::time::Instant::now() + TOTAL_TIMEOUT;
    loop {
        if rpc.get_blockchain_info().await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "bitcoind did not become ready within {TOTAL_TIMEOUT:?}"
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Poll bitcoind's mempool until `predicate(txids)` returns true.
pub async fn wait_for_mempool_pred<F>(
    rpc: &RpcClient,
    timeout_duration: Duration,
    mut predicate: F,
    label: &str,
) -> anyhow::Result<()>
where
    F: FnMut(&[Txid]) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let txids = mempool_txids(rpc).await?;
        if predicate(&txids) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for bitcoind mempool `{label}` after {timeout_duration:?}"
            ));
        }
        sleep(Duration::from_millis(75)).await;
    }
}

// Wallet/bitcoind RPCs not exposed by `MainClient`

/// `createwallet <name>`.
pub async fn create_wallet(
    rpc: &RpcClient,
    name: &str,
) -> Result<(), ClientError> {
    let _resp: serde_json::Value =
        rpc.request("createwallet", rpc_params![name]).await?;
    Ok(())
}

/// `sendtoaddress <addr> <amount>` — returns the new txid.
pub async fn send_to_address(
    rpc: &RpcClient,
    addr: &Address,
    amount_sat: u64,
) -> anyhow::Result<Txid> {
    let amount_btc = format!("{:.8}", (amount_sat as f64) / 100_000_000.0);
    let txid: String = rpc
        .request("sendtoaddress", rpc_params![addr.to_string(), amount_btc])
        .await?;
    Ok(txid.parse()?)
}

/// `bumpfee <txid>` — returns the replacement txid.
pub async fn bump_fee(rpc: &RpcClient, txid: Txid) -> anyhow::Result<Txid> {
    #[derive(serde::Deserialize)]
    struct BumpFee {
        txid: String,
    }
    let res: BumpFee = rpc
        .request("bumpfee", rpc_params![txid.to_string()])
        .await?;
    Ok(res.txid.parse()?)
}

/// `generateblock <addr> <txids>` — mines a single block including exactly
/// the given txs (use `[]` for an empty block of just coinbase).
pub async fn generate_block(
    rpc: &RpcClient,
    addr: &Address,
    txs: &[Txid],
) -> anyhow::Result<BlockHash> {
    let tx_strs: Vec<String> = txs.iter().map(|t| t.to_string()).collect();
    let res: serde_json::Value = rpc
        .request("generateblock", rpc_params![addr.to_string(), tx_strs])
        .await?;
    let hash = res["hash"]
        .as_str()
        .ok_or_else(|| anyhow!("generateblock missing `hash`: {res:?}"))?;
    Ok(hash.parse()?)
}

/// `getnewaddress` — returns a fresh wallet address.
pub async fn get_new_address(rpc: &RpcClient) -> anyhow::Result<Address> {
    let s: String = rpc.request("getnewaddress", rpc_params![]).await?;
    Ok(s.parse::<Address<_>>()?
        .require_network(bitcoin::Network::Regtest)?)
}

/// Send `amount_sat` to a fresh address — `getnewaddress` + `sendtoaddress`.
pub async fn submit_tx(
    rpc: &RpcClient,
    amount_sat: u64,
) -> anyhow::Result<Txid> {
    let dest = get_new_address(rpc).await?;
    send_to_address(rpc, &dest, amount_sat).await
}

/// `getrawmempool` — bitcoind's current mempool as a list of txids.
pub async fn mempool_txids(rpc: &RpcClient) -> anyhow::Result<Vec<Txid>> {
    // We bypass MainClient::get_raw_mempool because its typed
    // `Verbose`/`MempoolSequence` witnesses are unwieldy; a simple
    // `Vec<String>` parse is all we need.
    let txids: Vec<String> =
        rpc.request("getrawmempool", rpc_params![]).await?;
    txids
        .into_iter()
        .map(|s| s.parse::<Txid>().map_err(anyhow::Error::from))
        .collect()
}

#[derive(Clone, Debug)]
pub struct TestFailure {
    pub test_name: String,
    pub error: String,
    /// Optional directory whose `stdout.txt` / `stderr.txt` should be
    /// tailed onto the failure summary.
    pub log_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct TestFailureCollector {
    failures: Arc<Mutex<Vec<TestFailure>>>,
}

impl TestFailureCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_failure(&self, failure: TestFailure) {
        self.failures.lock().push(failure);
    }

    /// Print a banner with each failure, followed by the last
    /// lines of `stdout.txt` / `stderr.txt` from the failing
    /// test's `log_dir` (if any).
    #[expect(clippy::print_stderr)]
    pub fn display_all_failures(&self) {
        const TAIL_LINES: usize = 30;
        let failures = self.failures.lock();
        if failures.is_empty() {
            return;
        }
        eprintln!("\n{}", "=".repeat(80));
        eprintln!("TEST FAILURE SUMMARY ({} failed test(s))", failures.len());
        eprintln!("{}", "=".repeat(80));
        for (i, f) in failures.iter().enumerate() {
            eprintln!("\n[{}] {}", i + 1, f.test_name);
            eprintln!("    error: {}", f.error);
            if let Some(dir) = &f.log_dir {
                for stream in ["stdout.txt", "stderr.txt"] {
                    let path = dir.join(stream);
                    match read_file_tail(&path, TAIL_LINES) {
                        Ok(content) if !content.trim().is_empty() => {
                            eprintln!(
                                "    --- bitcoind {stream} (last {TAIL_LINES} lines, {}) ---",
                                path.display()
                            );
                            for line in content.lines() {
                                eprintln!("    | {line}");
                            }
                        }
                        Ok(_) => {}
                        Err(err) => eprintln!(
                            "    --- failed to read {}: {err} ---",
                            path.display()
                        ),
                    }
                }
            }
        }
        eprintln!("\n{}", "=".repeat(80));
    }
}

fn read_file_tail(path: &Path, n: usize) -> Result<String, std::io::Error> {
    let content = std::fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() <= n {
        Ok(content)
    } else {
        Ok(lines[lines.len() - n..].join("\n"))
    }
}
