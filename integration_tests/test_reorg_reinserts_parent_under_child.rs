//! A reorg puts a parent back into the mempool underneath a child that never
//! left, and `Mempool::insert` has to survive receiving them in that order.
//!
//! `insert` maintains ancestor statistics and the `by_ancestor_fee_rate` index
//! incrementally, which is only sound if parents arrive first. Confirming a
//! package's parent alone leaves the child behind as a root; invalidating that
//! block hands the parent back with the child already in place. Two things
//! then go wrong, and the test asserts both: the child's fee-rate key is not
//! updated to match its new ancestor stats, so the next `remove` — which every
//! `propose_txs` performs — fails and the GBT RPC serves an error rather than
//! a template; and the child's `depends` never records the returning parent,
//! so it is proposed as a dependency-free root.

use std::time::Duration;

use bitcoin::Txid;
use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::{TestSetup, local_block_template},
    util::{
        TemplateViolation, generate_block, submit_child_of,
        template_violations, wait_for_mempool_pred,
    },
};

/// Well above the 1 sat/vB floor, so the child's ancestor fee rate differs
/// from its own and a stale key is a distinguishable one.
const CHILD_FEE_SAT: u64 = 5_000;

pub async fn test_reorg_reinserts_parent_under_child(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let parent = setup.submit_and_wait(2_000_000).await?;
    let child =
        submit_child_of(&setup.node.rpc_client, parent, CHILD_FEE_SAT).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&parent) && t.contains(&child),
            "package in local mempool",
        )
        .await?;

    // `generateblock` rather than mining the whole mempool: the child has to
    // stay behind, as the root the parent later returns underneath.
    let block = generate_block(
        &setup.node.rpc_client,
        &setup.node.mining_address,
        &[parent],
    )
    .await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| !t.contains(&parent) && t.contains(&child),
            "parent confirmed, child left behind",
        )
        .await?;

    // Hand the parent back, underneath the child.
    setup.node.rpc_client.invalidate_block(block).await?;
    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&parent) && t.contains(&child),
        "package back in bitcoind mempool",
    )
    .await?;

    // Not `wait_for_local_mempool`: it turns a failed `propose_txs` into an
    // empty set, so the corruption would read as "the reorg dropped
    // everything" instead of naming itself.
    let template = wait_for_template(&setup, &[parent, child]).await?;

    let positions: Vec<Txid> = template.iter().map(|t| t.txid).collect();
    let parent_idx = positions.iter().position(|t| *t == parent);
    let child_idx = positions.iter().position(|t| *t == child);
    anyhow::ensure!(
        parent_idx < child_idx,
        "parent {parent} must be proposed before child {child}: {positions:?}"
    );

    // A template listing the child as dependency-free is one a miner may
    // truncate at the parent.
    let child_entry = template
        .iter()
        .find(|t| t.txid == child)
        .expect("child is in the template, just checked");
    anyhow::ensure!(
        child_entry
            .depends
            .contains(&(parent_idx.expect("parent is in the template") as u32)),
        "child {child} does not depend on parent {parent}: depends={:?}, \
         template={positions:?}",
        child_entry.depends,
    );

    let violations =
        template_violations(&setup.node.rpc_client, &template).await?;
    anyhow::ensure!(
        violations.is_empty(),
        "template is not minable after the reorg: {}",
        violations
            .iter()
            .map(TemplateViolation::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    );

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}

/// Poll `propose_txs` until it serves a template holding all of `expected`,
/// surfacing whatever error it fails with in the meantime.
async fn wait_for_template(
    setup: &TestSetup,
    expected: &[Txid],
) -> anyhow::Result<Vec<bitcoin_jsonrpsee::client::BlockTemplateTransaction>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let template = local_block_template(&setup.mempool_sync).await?;
        let txids: Vec<Txid> = template.iter().map(|t| t.txid).collect();
        if expected.iter().all(|txid| txids.contains(txid)) {
            return Ok(template);
        }
        let () = setup.task_errors.ensure_empty("template after the reorg")?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "template never came to hold {expected:?}; it holds {txids:?}"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
