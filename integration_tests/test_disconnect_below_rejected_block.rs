//! A block the enforcer rejects still advances the *unfiltered* mempool tip —
//! `connect_block` moves it before consulting the enforcer — and the
//! disconnect that `invalidateblock` then emits is ignored, because we never
//! connected the block. Disconnecting the accepted parent from underneath it
//! must still work: an unfiltered tip left stuck on the rejected child fails
//! the parent's disconnect with `DisconnectTipMismatch`, killing the task.

use std::time::Duration;

use bitcoin::BlockHash;
use bitcoin_jsonrpsee::MainClient as _;

use crate::{setup::TestSetup, util::get_new_address};

const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Poll until bitcoind's tip is `expected` again, i.e. the enforcer's
/// rejection has driven `invalidateblock` through.
async fn wait_for_invalidation(
    setup: &TestSetup,
    expected: BlockHash,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if setup.node.rpc_client.getbestblockhash().await? == expected {
            return Ok(());
        }
        let () = setup
            .task_errors
            .ensure_empty("invalidation of rejected block")?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "bitcoind never invalidated the rejected block, \
             tip never returned to {expected}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn test_disconnect_below_rejected_block(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let grandparent = setup.node.rpc_client.getbestblockhash().await?;

    // The parent, accepted: both the filtered and the unfiltered tip.
    let parent = setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?[0];
    setup
        .wait_for_local_tip(parent, Duration::from_secs(10))
        .await?;

    // The child, rejected: only the unfiltered tip follows it.
    setup.enforcer.set_reject_all_blocks(true);
    let rejected = setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?[0];
    tracing::info!(%rejected, %parent, "mined a block the enforcer rejects");

    let () = wait_for_invalidation(&setup, parent).await?;
    setup.enforcer.set_reject_all_blocks(false);

    // Disconnect the accepted parent, from below the rejected child.
    setup.node.rpc_client.invalidate_block(parent).await?;
    setup
        .wait_for_local_tip(grandparent, Duration::from_secs(15))
        .await?;

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
    tracing::info!(%reconnected, "mined a fresh block on the restored tip");

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
