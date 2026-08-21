//! A tx replaced while its fetch is in flight must not kill the sync task.
//!
//! `Removed` cancels a pending `RequestItem::Tx`, but only while it is still
//! queued. A fetch already in flight completes against a tx bitcoind has since
//! dropped, so `getrawtransaction` answers -5 (`RPC_INVALID_ADDRESS_OR_KEY`) —
//! which used to be a fatal `SyncTaskError::Request`.
//!
//! Made deterministic by stalling the fetch rather than racing it: it is held
//! open, one of its txs is RBF'd underneath it, and only then released. The test
//! is set up such that both batched and non-batched requests are covered.

use std::time::Duration;

use bitcoin::Txid;

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    stalling_client::StallingClient,
    util::{BinPaths, RpcClient, bump_fee, submit_tx, wait_for_mempool_pred},
};

const TX_FETCH_METHOD: &str = "getrawtransaction";

/// How the fetch that loses the race is dispatched.
#[derive(Clone, Copy, Debug)]
pub enum FetchShape {
    /// A lone queued `RequestItem::Tx`, sent as a single `getrawtransaction`.
    Single,
    /// Several queued, drained into one batched `getrawtransaction`.
    Batched,
}

/// A fetch parked in flight by [`park_fetch`].
struct ParkedFetch {
    /// The tx in it to replace.
    replaced: Txid,
    /// The other txs in the same fetch. Nothing about them changed, so they
    /// must survive the replacement and still reach the local mempool — the
    /// half of the batch bug that `into_ok()` broke.
    others: Vec<Txid>,
}

/// Wait for the armed fetch to actually be parked. Without this the RBF could
/// land before the sync task has even asked, so the fetch would never be in
/// flight and the bug would not be exercised at all.
async fn wait_parked(
    stalling: &StallingClient,
    what: &str,
) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), stalling.wait_until_stalled())
        .await
        .map_err(|_| anyhow::anyhow!("sync task never {what}"))
}

/// Submit txs and hold the sync task's fetch for them open, so the caller can
/// change bitcoind's mempool underneath it.
async fn park_fetch(
    rpc: &RpcClient,
    stalling: &StallingClient,
    shape: FetchShape,
) -> anyhow::Result<ParkedFetch> {
    // Whatever the shape, the first thing to park is a single fetch.
    stalling.arm();
    let first = submit_tx(rpc, 50_000).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(5),
        |t| t.contains(&first),
        "first tx in bitcoind mempool",
    )
    .await?;
    wait_parked(stalling, &format!("fetched {first}")).await?;

    let FetchShape::Batched = shape else {
        return Ok(ParkedFetch {
            replaced: first,
            others: Vec::new(),
        });
    };

    // For the batched shape that first fetch is only a wedge: `StreamExt::
    // then` does not poll the request queue while a fetch is in flight, so txs
    // submitted now accumulate behind it instead of each going out as its own
    // single request.
    //
    // `other` before `replaced`: bumping `replaced` must not evict `other`,
    // which it would if `other` were the one spending `replaced`'s change.
    let other = submit_tx(rpc, 45_000).await?;
    let replaced = submit_tx(rpc, 40_000).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(5),
        |t| t.contains(&other) && t.contains(&replaced),
        "txs to batch in bitcoind mempool",
    )
    .await?;

    // bitcoind having them says nothing about the sync task having seen the
    // ZMQ `Added` for both. Releasing in between would send the second as its
    // own single request and never reach the batch path.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Release the wedge; the next request drains both queued txs into one
    // batch, which is parked in its place.
    stalling.arm_batch();
    stalling.release();
    wait_parked(stalling, &format!("issued a batched `{TX_FETCH_METHOD}`"))
        .await?;

    // Prove it is a batch, and one carrying both queued txs rather than some
    // incidental pair — otherwise this shape silently degrades into the single
    // one and asserts nothing new. Checking the count alone is not enough: if
    // the `Added` for one of them arrived after the wedge was released it
    // would go out on its own, and a batch of the other two would still pass.
    let batch = stalling.stalled_batch();
    let fetches: Vec<&str> = batch
        .iter()
        .filter(|m| m.method == TX_FETCH_METHOD)
        .map(|m| m.params.as_str())
        .collect();
    anyhow::ensure!(
        fetches.len() >= 2,
        "stalled request was not a batched fetch of several txs: {batch:?}"
    );
    for txid in [other, replaced] {
        anyhow::ensure!(
            fetches.iter().any(|p| p.contains(&txid.to_string())),
            "batched fetch did not carry {txid}, so the batch path was not \
             entered for it: {batch:?}"
        );
    }

    Ok(ParkedFetch {
        replaced,
        others: vec![other],
    })
}

pub async fn test_tx_replaced_during_fetch(
    bin_paths: BinPaths,
    directories: Directories,
    shape: FetchShape,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    // The sync task fetches through the stalling client; the test keeps
    // driving bitcoind directly. Armed only after init-sync, so the stall
    // lands on our txs rather than on init's own fetches.
    let stalling = StallingClient::new(node.rpc_client.clone());
    let setup = TestSetup::start_with_client(node, stalling.clone()).await?;
    let rpc = &setup.node.rpc_client;

    let parked = park_fetch(rpc, &stalling, shape).await?;

    // Replace one of its txs while the fetch is open, so the in-flight
    // `getrawtransaction` resolves against a tx bitcoind no longer has.
    let replacement = bump_fee(rpc, parked.replaced).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| {
            t.contains(&replacement)
                && !t.contains(&parked.replaced)
                && parked.others.iter().all(|o| t.contains(o))
        },
        "replacement in bitcoind mempool, rest of the fetch intact",
    )
    .await?;

    stalling.release();

    // The task must survive and keep applying actions: a fresh tx arriving
    // locally is the positive signal, a dead task would never pick it up. The
    // rest of the fetch must arrive too — losing the race for one tx must not
    // cost the others.
    let probe = submit_tx(rpc, 35_000).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| {
                t.contains(&probe)
                    && parked.others.iter().all(|o| t.contains(o))
            },
            "probe tx and the rest of the parked fetch in the local mempool",
        )
        .await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
