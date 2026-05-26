//! `accept_tx` runs over both the initial-sync filter pass (for pre-existing
//! mempool txs) and the running task path (for txs added afterwards).

use std::time::Duration;

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    util::{BinPaths, submit_tx, wait_for_mempool_pred},
};

pub async fn test_accept_tx_paths(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    // Seed a tx BEFORE init_sync runs; the initial mempool-filter pass must
    // route it through accept_tx.
    let pre_sync_tx = submit_tx(&node.rpc_client, 50_000).await?;
    wait_for_mempool_pred(
        &node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&pre_sync_tx),
        "seed tx in bitcoind mempool",
    )
    .await?;

    let setup = TestSetup::start(node).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(5),
            |t| t.contains(&pre_sync_tx),
            "pre-sync tx in local mempool after init_sync",
        )
        .await?;

    let post_sync_tx = setup.submit_and_wait(50_000).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let calls = setup.enforcer.accept_tx_calls();
        if calls.contains(&pre_sync_tx) && calls.contains(&post_sync_tx) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "expected accept_tx for both {pre_sync_tx} (pre-sync) and \
                 {post_sync_tx} (post-sync); saw {calls:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
