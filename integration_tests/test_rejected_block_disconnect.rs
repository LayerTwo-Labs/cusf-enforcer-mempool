//! A block the enforcer rejects is invalidated in bitcoind, which then emits a
//! disconnect for it. The sync task must not stall on that disconnect, and
//! must afterward accept a *new* block mined on the resulting tip.

use std::time::Duration;

use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::TestSetup,
    util::{get_new_address, submit_tx},
};

/// Comfortably longer than the sync task's `APPLY_SYNC_ACTION_TIMEOUT` (15s),
/// so a stalled action has time to fail the task and be *observed* failing,
/// rather than the test merely outrunning it. Nothing waits this long on
/// success -- each step returns as soon as its outcome is visible.
const PAST_APPLY_TIMEOUT: Duration = Duration::from_secs(25);

const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Poll until bitcoind's tip is no longer `block_hash`, i.e. the enforcer's
/// rejection has driven `invalidateblock` through.
async fn wait_for_invalidation(
    setup: &TestSetup,
    block_hash: bitcoin::BlockHash,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if setup.node.rpc_client.getbestblockhash().await? != block_hash {
            return Ok(());
        }
        let () = setup
            .task_errors
            .ensure_empty("invalidation of rejected block")?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "bitcoind never invalidated the rejected block {block_hash}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn test_rejected_block_disconnect(
    setup: TestSetup,
) -> anyhow::Result<()> {
    // A tx in the mempool, so the rejected block carries one. Invalidating the
    // block hands it back to us, and since we never connected the block we
    // never removed it -- so this also covers re-announcement of a tx we still
    // hold.
    let before = setup.submit_and_wait(50_000).await?;

    setup.enforcer.set_reject_all_blocks(true);
    let rejected = setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?[0];
    tracing::info!(%rejected, "mined a block the enforcer will reject");

    let () = wait_for_invalidation(&setup, rejected).await?;
    setup.enforcer.set_reject_all_blocks(false);
    tracing::info!(%rejected, "rejected block was invalidated");

    let probe = submit_tx(&setup.node.rpc_client, 40_000).await?;
    let () = setup
        .wait_for_local_mempool(
            PAST_APPLY_TIMEOUT,
            |txids| txids.contains(&probe),
            "tx accepted after a disconnect for a never-connected block",
        )
        .await?;

    anyhow::ensure!(
        setup.local_mempool_txids().await.contains(&before),
        "the tx returned by the invalidation should still be in the local mempool"
    );

    // A fresh address, not `setup.node.mining_address` again: reusing it
    // right after `invalidateblock` at the same height can produce a
    // byte-identical coinbase (and hence block hash) to the one just
    // invalidated, which bitcoind then refuses as `duplicate-invalid`
    // rather than actually connecting a new block.
    let reconnect_address = get_new_address(&setup.node.rpc_client).await?;
    let reconnected = setup
        .node
        .rpc_client
        .generate_to_address(1, &reconnect_address.into_unchecked())
        .await?[0];
    tracing::info!(%reconnected, "mined a fresh block on the real tip");

    setup
        .wait_for_local_tip(reconnected, Duration::from_secs(10))
        .await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "MempoolSync task surfaced errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
