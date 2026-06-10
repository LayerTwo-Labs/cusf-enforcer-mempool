//! A ZMQ `Removed` event for a tx that's not in our local mempool must NOT
//! stall the action queue.

use std::time::Duration;

use crate::{
    setup::TestSetup,
    util::{bump_fee, submit_tx, wait_for_mempool_pred},
};

pub async fn test_rbf_removed_for_absent_tx(
    setup: TestSetup,
) -> anyhow::Result<()> {
    // Enforcer rejects everything; the tx lands in bitcoind's mempool but
    // not ours.
    setup.enforcer.set_reject_all(true);

    let tx_rejected = submit_tx(&setup.node.rpc_client, 50_000).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&tx_rejected),
        "rejected tx in bitcoind mempool",
    )
    .await?;

    // Wait until the task has consulted the enforcer (so RejectTx is queued
    // before we bump the fee).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !setup.enforcer.accept_tx_calls().contains(&tx_rejected) {
        let () = setup
            .task_errors
            .ensure_empty(&format!("accept_tx({tx_rejected})"))?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "sync task never asked enforcer about {tx_rejected}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::ensure!(
        !setup.local_mempool_txids().await.contains(&tx_rejected),
        "rejected tx should be absent from local mempool"
    );

    // RBF the rejected tx → bitcoind emits `Removed(tx_rejected)` (REPLACED)
    // for a tx our local mempool doesn't contain. Action queue should stall
    // on Pending.
    let tx_replacement = bump_fee(&setup.node.rpc_client, tx_rejected).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&tx_replacement) && !t.contains(&tx_rejected),
        "replacement in bitcoind mempool",
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Flip the enforcer back to accepting and verify a fresh tx propagates.
    setup.enforcer.set_reject_all(false);
    let _probe = setup.submit_and_wait(40_000).await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
