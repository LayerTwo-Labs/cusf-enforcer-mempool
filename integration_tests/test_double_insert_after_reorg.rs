//! When `handle_disconnected_block` defers a tx via `InsertTx` (because its
//! parent isn't in `tx_cache`) AND bitcoind's reorg-recovery re-broadcasts
//! the same tx via ZMQ `Added`, `mempool.insert` is called twice and the
//! second call returns `TxAlreadyExists`.
//!
//! To force the deferred path: confirm the tx into a block BEFORE
//! `init_sync_mempool` runs, so the sync task starts with an empty
//! `tx_cache` and the disconnect handler can't find the tx's coinbase
//! parent.

use std::time::Duration;

use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    util::{BinPaths, submit_tx, wait_for_mempool_pred},
};

pub async fn test_double_insert_after_reorg(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    let tx = submit_tx(&node.rpc_client, 50_000).await?;
    wait_for_mempool_pred(
        &node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&tx),
        "seed tx in bitcoind mempool",
    )
    .await?;
    node.rpc_client
        .generate_to_address(1, &node.mining_address.clone().into_unchecked())
        .await?;

    let setup = TestSetup::start(node).await?;
    let tip_with_tx = setup.node.rpc_client.getbestblockhash().await?;
    setup.node.rpc_client.invalidate_block(tip_with_tx).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut errors = Vec::<String>::new();
    while tokio::time::Instant::now() < deadline {
        errors = setup.task_errors.snapshot();
        if !errors.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if errors
        .iter()
        .any(|e| e.contains("already exists in mempool"))
    {
        anyhow::bail!(
            "MempoolSync task aborted with TxAlreadyExists: {errors:?}"
        );
    }
    anyhow::ensure!(
        errors.is_empty(),
        "MempoolSync task surfaced unexpected errors: {errors:?}"
    );
    setup
        .wait_for_local_mempool(
            Duration::from_secs(5),
            |t| t.contains(&tx),
            "tx in local mempool after deferred-reorg recovery",
        )
        .await?;
    Ok(())
}
