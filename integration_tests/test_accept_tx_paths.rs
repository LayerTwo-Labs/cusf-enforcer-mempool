//! `accept_tx` runs over both the initial-sync filter pass (for pre-existing
//! mempool txs) and the running task path (for txs added afterwards).
//!
//! Also covers enforcer-level conflicts reported by `accept_tx`: the
//! lower-fee-rate side must be deprioritized in Bitcoin Core so its
//! `getblocktemplate` stops offering both. Exercised in both directions.

use std::{collections::HashSet, time::Duration};

use bitcoin::Txid;

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    util::{
        BinPaths, prioritised_txids, submit_tx, submit_tx_with_fee_rate,
        wait_for_mempool_pred,
    },
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
            break;
        }
        let () = setup
            .task_errors
            .ensure_empty("accept_tx for pre- and post-sync txs")?;
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "expected accept_tx for both {pre_sync_tx} (pre-sync) and \
                 {post_sync_tx} (post-sync); saw {calls:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }

    // An enforcer-level conflict (see `RequestItem::DeprioritizeTx`): Core
    // cannot see it, so the lower-ancestor-fee-rate side must be
    // deprioritized there, or `getblocktemplate` offers both and any block
    // built from the template is invalid.
    //
    // Fee rates far enough apart pin the expected winner even if the wallet
    // funds one tx from the other's unconfirmed change: the child's ancestor
    // fee rate lands strictly between the two rates.
    const LOW_FEE_RATE: u64 = 2; // sat/vB
    const HIGH_FEE_RATE: u64 = 25; // sat/vB

    // A higher-fee-rate newcomer buries the incumbent.
    let incumbent = setup
        .submit_and_wait_with_fee_rate(20_000, LOW_FEE_RATE)
        .await?;
    anyhow::ensure!(
        !prioritised_txids(&setup.node.rpc_client)
            .await?
            .contains(&incumbent),
        "{incumbent} should not be deprioritized before any conflict is \
         reported"
    );
    setup
        .enforcer
        .set_conflicts_for_next(HashSet::from([incumbent]));
    // Not `submit_and_wait`: a losing tx is deprioritized to the point of
    // mempool eviction, so waiting for it locally would race the fix.
    let newcomer =
        submit_tx_with_fee_rate(&setup.node.rpc_client, 20_000, HIGH_FEE_RATE)
            .await?;
    expect_conflict_loser(&setup, incumbent, newcomer).await?;

    // A lower-fee-rate newcomer loses to the incumbent.
    let incumbent = setup
        .submit_and_wait_with_fee_rate(20_000, HIGH_FEE_RATE)
        .await?;
    setup
        .enforcer
        .set_conflicts_for_next(HashSet::from([incumbent]));
    let newcomer =
        submit_tx_with_fee_rate(&setup.node.rpc_client, 20_000, LOW_FEE_RATE)
            .await?;
    expect_conflict_loser(&setup, newcomer, incumbent).await?;

    Ok(())
}

/// Wait until `loser` is deprioritized in bitcoind, checking all the while
/// that `winner` is not.
async fn expect_conflict_loser(
    setup: &TestSetup,
    loser: Txid,
    winner: Txid,
) -> anyhow::Result<()> {
    let check_winner = |prioritised: &HashSet<Txid>| -> anyhow::Result<()> {
        anyhow::ensure!(
            !prioritised.contains(&winner),
            "{winner} has the higher fee rate and must not be deprioritized \
             for its conflict with {loser}"
        );
        Ok(())
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let prioritised = prioritised_txids(&setup.node.rpc_client).await?;
        check_winner(&prioritised)?;
        if prioritised.contains(&loser) {
            // Give a buggy implementation a moment to also deprioritize the
            // winner before declaring success.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let prioritised = prioritised_txids(&setup.node.rpc_client).await?;
            check_winner(&prioritised)?;
            tracing::info!(
                %loser,
                %winner,
                "conflict loser deprioritized in bitcoind"
            );
            return Ok(());
        }
        let () = setup
            .task_errors
            .ensure_empty("deprioritizing the losing side of a conflict")?;
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "{loser} was not deprioritized in bitcoind after `accept_tx` \
                 reported its conflict with {winner}; Core would offer both \
                 in one template. Currently deprioritized: {prioritised:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
