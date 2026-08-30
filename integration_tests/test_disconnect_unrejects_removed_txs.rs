//! The mirror of `test_connect_block_deprioritizes_removed_txs`: a block that
//! removed mempool txs under the enforcer's rules is disconnected, so the
//! reason those txs were removed is gone and the deprioritisation must go with
//! it.
//!
//! Without the undo, a BMM bid that loses its auction stays deprioritized on
//! the node forever, even after the block that beat it is invalidated and the
//! bid is targeting the tip again. It is nominally resident in bitcoind's
//! mempool but at an unmineable feerate, and absent from our own filtered
//! mempool, so no template will ever select it.

use std::{collections::HashSet, time::Duration};

use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::TestSetup,
    util::{generate_block, modified_fee_sat},
};

pub async fn test_disconnect_unrejects_removed_txs(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    let tx_stale = setup.submit_and_wait(50_000).await?;
    let pre_block_tip = rpc.getbestblockhash().await?;
    let baseline = modified_fee_sat(rpc, tx_stale).await?;
    anyhow::ensure!(
        baseline > 0,
        "expected {tx_stale} to start with a positive modified fee, got {baseline} sat"
    );

    setup
        .enforcer
        .set_always_remove_on_connect(HashSet::from([tx_stale]));

    // Empty block: `tx_stale` is removed by enforcer policy, not by
    // confirmation, so it stays in bitcoind's mempool throughout.
    let block = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;

    // Precondition: the connect path did deprioritize it. Shares the request
    // queue with the tip update, so it lands shortly after.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if modified_fee_sat(rpc, tx_stale).await? < 0 {
            break;
        }
        let () = setup
            .task_errors
            .ensure_empty("deprioritisation of removed tx")?;
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "expected {tx_stale} to be deprioritized by the connected block"
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    anyhow::ensure!(
        !setup.local_mempool_txids().await.contains(&tx_stale),
        "{tx_stale} should be gone from the local mempool mirror"
    );

    rpc.invalidate_block(block).await?;
    setup
        .wait_for_local_tip(pre_block_tip, Duration::from_secs(10))
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let modified = loop {
        let modified = modified_fee_sat(rpc, tx_stale).await?;
        if modified >= 0 {
            break modified;
        }
        let () = setup
            .task_errors
            .ensure_empty("undo of deprioritisation on disconnect")?;

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Expected the deprioritisation of {tx_stale} to be undone \
                 after the block that removed it was disconnected. Still at \
                 {modified} sat (baseline {baseline} sat). Fee-driven mining \
                 would never select it again."
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    };
    anyhow::ensure!(
        modified == baseline,
        "expected {tx_stale} back at its original modified fee of {baseline} \
         sat, got {modified} sat: the undo did not cancel the earlier delta"
    );

    // The tx never left bitcoind's mempool, so nothing re-announces it. It is
    // mineable again only if the disconnect put it back itself.
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |txids| txids.contains(&tx_stale),
            "tx re-inserted into local mempool after disconnect",
        )
        .await?;

    let () = setup.task_errors.ensure_empty("end of test")?;
    Ok(())
}
