//! An enforcer that reports a block as invalid from `sync_to_tip` must not
//! abort the initial sync. `initial_sync` invalidates the reported block on
//! bitcoind, re-syncs against the resulting tip, and the mempool task must
//! come up healthy — in particular, the disconnect the invalidation produces
//! must not be left in the sequence stream to poison the task afterwards.

use std::time::Duration;

use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    mock_enforcer::MockEnforcer,
    setup::{RegtestNode, TestSetup},
    util::BinPaths,
};

pub async fn test_invalid_block_during_initial_sync(
    bin_paths: BinPaths,
    dirs: crate::setup::Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, dirs).await?;

    // The priming tip is the block the enforcer will report as invalid during
    // the initial sync.
    let invalid_tip = node.rpc_client.getbestblockhash().await?;
    let expected_tip = node
        .rpc_client
        .getblockheader(invalid_tip)
        .await?
        .prev_blockhash;

    let enforcer = MockEnforcer::new();
    enforcer.report_invalid_block_on_sync(invalid_tip);

    // The initial sync runs here. It must survive the invalid block instead
    // of erroring out.
    let setup = TestSetup::start_with_enforcer(node, enforcer).await?;

    let node_tip = setup.node.rpc_client.getbestblockhash().await?;
    anyhow::ensure!(
        node_tip == expected_tip,
        "initial sync should have invalidated {invalid_tip} on bitcoind, \
         but its tip is {node_tip} (expected {expected_tip})"
    );

    let sync_calls = setup.enforcer.sync_to_tip_calls();
    anyhow::ensure!(
        sync_calls.first() == Some(&invalid_tip)
            && sync_calls.len() >= 2
            && sync_calls[1..].iter().all(|tip| *tip == expected_tip),
        "expected a sync attempt at the invalid tip, then re-sync(s) at the \
         reorged tip, got: {sync_calls:?}"
    );

    let () = setup
        .wait_for_local_tip(expected_tip, Duration::from_secs(5))
        .await?;

    // The task must remain healthy after the initial sync: a tx flows into
    // the local mempool, and a block mined on the reorged tip connects, which
    // would fail if the invalidation's disconnect message were still queued
    // in the sequence stream. The tx also makes the mined block distinct from
    // the invalidated one — a regtest coinbase-only block on the same parent
    // would be byte-identical to it, and rejected as `duplicate-invalid`.
    let _txid = setup.submit_and_wait(50_000).await?;
    let new_block = setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?[0];
    let () = setup
        .wait_for_local_tip(new_block, Duration::from_secs(10))
        .await?;

    let () = setup.task_errors.ensure_empty("end of test")?;
    Ok(())
}
