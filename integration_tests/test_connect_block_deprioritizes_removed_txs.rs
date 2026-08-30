//! When a connected block makes mempool txs invalid under the enforcer's
//! rules, `connect_block` reports them in `remove_mempool_txs`. Dropping them
//! from our own mempool mirror is not enough: bitcoind still holds them at
//! their original feerate, so ordinary fee-driven `getblocktemplate`
//! construction keeps selecting them. They must be deprioritized on the node too,
//! via the same `RejectTx` sync action (`prioritisetransaction`) the `accept_tx`
//! rejection path uses.
//!
//! The concrete case this protects is a BMM bid that loses its auction: the
//! bid targets one mainchain tip, a block supersedes that tip, and the bid is
//! consensus-dead from then on -- but it stays nominally resident in
//! bitcoind's mempool at full fee, which for a BMM bid is deliberately large.
//!
//! An enforcer may also name a tx the block *mined* in `remove_mempool_txs`
//! (the winning bid is removed along with the losers). Applying the block
//! already removed that tx non-cascadingly, so removing it a second time --
//! this time with descendants -- would evict children that the block left
//! perfectly valid. This test covers that too.

use std::{collections::HashSet, time::Duration};

use crate::{
    setup::TestSetup,
    util::{
        generate_block, modified_fee_sat, prioritised_txids, submit_child_tx,
    },
};

pub async fn test_connect_block_deprioritizes_removed_txs(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    let tx_mined = setup.submit_and_wait(40_000).await?;
    // An unconfirmed child of the tx that the block confirms. Mining its parent
    // does not invalidate it, so it must survive the connect even though the
    // enforcer names the parent in `remove_mempool_txs`.
    let tx_child = submit_child_tx(rpc, tx_mined, 1_000).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(5),
            |txids| txids.contains(&tx_child),
            "child of the mined tx",
        )
        .await?;
    let tx_stale = setup.submit_and_wait(50_000).await?;

    // Verify the bid really is attractive to fee-driven
    // template construction before the block lands.
    let baseline = modified_fee_sat(rpc, tx_stale).await?;
    anyhow::ensure!(
        baseline > 0,
        "expected {tx_stale} to start with a positive modified fee, got {baseline} sat"
    );
    anyhow::ensure!(
        !prioritised_txids(rpc).await?.contains(&tx_stale),
        "expected no pre-existing prioritisation delta for {tx_stale}"
    );

    setup
        .enforcer
        .set_always_remove_on_connect(HashSet::from([tx_mined, tx_stale]));

    let block =
        generate_block(rpc, &setup.node.mining_address, &[tx_mined]).await?;

    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;

    let local_txids = setup.local_mempool_txids().await;
    anyhow::ensure!(
        !local_txids.contains(&tx_stale),
        "{tx_stale} should be gone from the local mempool mirror"
    );

    // The mined tx was already removed non-cascadingly as part of applying the
    // block, so the `remove_mempool_txs` entry naming it must be skipped
    // rather than cascaded: cascading evicts descendants that are still valid.
    anyhow::ensure!(
        local_txids.contains(&tx_child),
        "{tx_child} spends the mined {tx_mined} and is still valid, but it was \
         dropped from the local mempool mirror; it would be missing from every \
         block template we build"
    );

    // `RejectTx` goes through the sync task's request queue, so the RPC lands
    // shortly after the tip does.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let modified = loop {
        let modified = modified_fee_sat(rpc, tx_stale).await?;
        if modified < 0 {
            break modified;
        }
        let () = setup
            .task_errors
            .ensure_empty("deprioritisation of removed tx")?;

        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Expected {tx_stale} to be deprioritized to a negative \
                 modified fee after the connected block removed it. Still at \
                 {modified} sat (baseline {baseline} sat). Fee-driven mining \
                 would keep selecting it."
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    };
    tracing::info!(%tx_stale, modified, "removed tx was deprioritized");

    // The winner, confirmed by this same block, must be left alone.
    let prioritised = prioritised_txids(rpc).await?;
    anyhow::ensure!(
        prioritised.contains(&tx_stale),
        "expected a prioritisation delta for {tx_stale}, got {prioritised:?}"
    );

    anyhow::ensure!(
        !prioritised.contains(&tx_mined),
        "{tx_mined} was confirmed by the connected block and must not be deprioritized"
    );

    let () = setup.task_errors.ensure_empty("end of test")?;
    Ok(())
}
