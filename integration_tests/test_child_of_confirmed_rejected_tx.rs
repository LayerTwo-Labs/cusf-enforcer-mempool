//! A tx the enforcer rejects leaves a tombstone in `rejected_txs`. Confirming
//! that tx must clear the tombstone: otherwise every later child of the now
//! confirmed tx is rejected in turn, and never reaches the local mempool or a
//! block template.

use std::time::Duration;

use crate::{
    setup::TestSetup,
    util::{
        generate_block, get_new_address, send_to_address, spend_wallet_output,
        wait_for_mempool_pred,
    },
};

pub async fn test_child_of_confirmed_rejected_tx(
    setup: TestSetup,
) -> anyhow::Result<()> {
    // Enforcer rejects everything, so the parent is tombstoned instead of
    // being added to our mempool.
    setup.enforcer.set_reject_all(true);

    let parent_dest = get_new_address(&setup.node.rpc_client).await?;
    let parent =
        send_to_address(&setup.node.rpc_client, &parent_dest, 100_000).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&parent),
        "parent tx in bitcoind mempool",
    )
    .await?;

    // Wait for the rejection itself, not just the broadcast.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !setup.enforcer.accept_tx_calls().contains(&parent) {
        let () = setup
            .task_errors
            .ensure_empty(&format!("accept_tx({parent})"))?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "sync task never asked enforcer about {parent}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::ensure!(
        !setup.local_mempool_txids().await.contains(&parent),
        "rejected parent should be absent from local mempool"
    );

    // Confirm the rejected parent. It has to be named explicitly, since the
    // `RejectTx` deprioritisation keeps it out of a fee-selected template.
    setup.enforcer.set_reject_all(false);
    let block_hash = generate_block(
        &setup.node.rpc_client,
        &setup.node.mining_address,
        &[parent],
    )
    .await?;
    setup
        .wait_for_local_tip(block_hash, Duration::from_secs(10))
        .await?;

    // Spend the confirmed parent. If the tombstone survives confirmation,
    // `try_get_parent_txs_from_caches` reports `Rejected(parent)` for the
    // child, which is then tombstoned too and never proposed.
    let child = spend_wallet_output(
        &setup.node.rpc_client,
        parent,
        &parent_dest,
        10_000,
    )
    .await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&child),
            "child of the confirmed parent in local mempool",
        )
        .await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
