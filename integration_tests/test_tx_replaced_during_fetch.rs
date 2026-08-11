//! A tx replaced while its fetch is in flight must not kill the sync task.
//!
//! `Removed` cancels a pending `RequestItem::Tx`, but only while it is still
//! queued. A request already in flight completes against a tx bitcoind has
//! since dropped, so `getrawtransaction` answers -5
//! (`RPC_INVALID_ADDRESS_OR_KEY`). That used to surface as a fatal
//! `SyncTaskError::Request` and take the block template server down with it.
//!
//! Made deterministic by stalling the fetch rather than racing it: the tx's
//! `getrawtransaction` is held open, the tx is RBF'd underneath it, and only
//! then is the fetch released — so it is guaranteed to be in flight when the
//! replacement lands.

use std::time::Duration;

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    stalling_client::StallingClient,
    util::{BinPaths, bump_fee, submit_tx, wait_for_mempool_pred},
};

pub async fn test_tx_replaced_during_fetch(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    // The sync task fetches through the stalling client; the test keeps
    // driving bitcoind through `node.rpc_client`. Armed only after init-sync,
    // so the stall lands on our tx rather than on init's own fetches.
    let stalling = StallingClient::new(node.rpc_client.clone());
    let setup = TestSetup::start_with_client(node, stalling.clone()).await?;
    stalling.arm();

    let tx_replaced = submit_tx(&setup.node.rpc_client, 50_000).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&tx_replaced),
        "seed tx in bitcoind mempool",
    )
    .await?;

    // Wait for the fetch to actually be parked. Without this the RBF could
    // land before the sync task has even asked, and the fetch would never be
    // in flight — the bug would not be exercised at all.
    tokio::time::timeout(
        Duration::from_secs(10),
        stalling.wait_until_stalled(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("sync task never fetched {tx_replaced}"))?;

    // Replace it while its fetch is open, so the in-flight `getrawtransaction`
    // resolves against a tx bitcoind no longer has.
    let tx_replacement = bump_fee(&setup.node.rpc_client, tx_replaced).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&tx_replacement) && !t.contains(&tx_replaced),
        "replacement in bitcoind mempool",
    )
    .await?;

    stalling.release();

    // The task must survive and keep applying actions. A fresh tx arriving
    // locally is the positive signal — a dead task would never pick it up.
    let _probe = setup.submit_and_wait(40_000).await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
