//! Asserts two related behaviors:
//!
//! 1. When a reorg disconnects a block and the enforcer rejects a
//!    re-inserted block tx, the sync task must stay healthy.
//! 2. The enforcer rejection leaves bitcoind with the tx (low priority)
//!    but us without it. After RBF emits a real `Removed(tx)`, a fresh
//!    probe must STILL propagate, i.e. `Removed` for an absent tx must
//!    not stall the queue.

use std::time::Duration;

use bitcoin_jsonrpsee::{MainClient as _, client::GetBlockClient as _};

use crate::{
    setup::TestSetup,
    util::{bump_fee, wait_for_mempool_pred},
};

pub async fn test_enforcer_rejection_during_reorg(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let tx = setup.submit_and_wait(50_000).await?;

    let pre_tip = setup.node.rpc_client.getbestblockhash().await?;
    let b1 = setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?[0];
    anyhow::ensure!(
        setup
            .node
            .rpc_client
            .get_block(b1, bitcoin_jsonrpsee::client::U8Witness::<2>)
            .await?
            .tx
            .iter()
            .any(|t| t.txid == tx),
        "expected B1 to confirm {tx}"
    );
    setup.wait_for_local_tip(b1, Duration::from_secs(5)).await?;

    // Reject the tx. The disconnect-recovery path is what calls
    // `accept_tx(tx)` next.
    setup.enforcer.reject_tx(tx);
    setup.node.rpc_client.invalidate_block(b1).await?;

    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&tx),
        "tx re-broadcast into bitcoind mempool",
    )
    .await?;
    setup
        .wait_for_local_tip(pre_tip, Duration::from_secs(10))
        .await?;
    anyhow::ensure!(
        !setup.local_mempool_txids().await.contains(&tx),
        "tx should be absent from local mempool after rejection"
    );

    // Assertion 1: a fresh tx still propagates immediately after the
    // rejection.
    let _probe1 = setup.submit_and_wait(40_000).await?;

    // Wait for RejectTx to fire (the `prioritisetransaction` RPC) before
    // bumping, otherwise bumpfee may compute the new fee against an
    // un-prioritised modified fee.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let tx_replacement = bump_fee(&setup.node.rpc_client, tx).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&tx_replacement) && !t.contains(&tx),
        "replacement in bitcoind mempool (REPLACED)",
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Assertion 2: a fresh tx still propagates after `Removed(tx)` for our
    // absent subject tx.
    let _probe2 = setup.submit_and_wait(30_000).await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "MempoolSync task surfaced errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
