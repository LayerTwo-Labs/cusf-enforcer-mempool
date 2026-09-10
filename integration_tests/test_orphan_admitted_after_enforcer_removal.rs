//! A tx the enforcer removes at block connect takes its descendants with it,
//! and those descendants must be recorded as rejected too.
//!
//! `connect_block` marks only the txid the enforcer named, so a dropped
//! descendant is absent from the enforced mempool, still held by the node,
//! still in `tx_cache`, and unmarked. A later child of it resolves that parent
//! straight out of `tx_cache` and is admitted with nothing to depend on,
//! leaving a template whose input no earlier tx creates.

use std::{collections::HashSet, time::Duration};

use bitcoin::Txid;
use bitcoin_jsonrpsee::client::BlockTemplateTransaction;

use crate::{
    setup::{TestSetup, local_block_template},
    util::{
        TemplateViolation, generate_block, submit_child_of,
        template_violations, wait_for_mempool_pred,
    },
};

const CHAINED_FEE_SAT: u64 = 5_000;

pub async fn test_orphan_admitted_after_enforcer_removal(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    let parent = setup.submit_and_wait(2_000_000).await?;
    let child = submit_child_of(rpc, parent, CHAINED_FEE_SAT).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&parent) && t.contains(&child),
            "package in local mempool",
        )
        .await?;

    // The enforcer refuses the parent at the next connect. The child goes with
    // it as a descendant; the node keeps both.
    setup
        .enforcer
        .set_always_remove_on_connect(HashSet::from([parent]));
    let first = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(first, Duration::from_secs(10))
        .await?;
    wait_for_template(&setup, |txids| {
        !txids.contains(&parent) && !txids.contains(&child)
    })
    .await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&parent) && t.contains(&child),
        "package still in bitcoind mempool",
    )
    .await?;

    // The node is happy to take a child of a tx it still holds.
    let grandchild = submit_child_of(rpc, child, CHAINED_FEE_SAT).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&grandchild),
        "grandchild in bitcoind mempool",
    )
    .await?;

    // Barrier: this block's connect action is queued behind the grandchild's,
    // so reaching it locally means the grandchild has been decided.
    let second = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(second, Duration::from_secs(10))
        .await?;

    let template = local_block_template(&setup.mempool_sync).await?;
    let txids: Vec<Txid> = template.iter().map(|t| t.txid).collect();
    anyhow::ensure!(
        !txids.contains(&grandchild),
        "grandchild {grandchild} was admitted though its parent {child} was \
         removed with {parent}; template holds {txids:?}"
    );

    let violations = template_violations(rpc, &template).await?;
    anyhow::ensure!(
        violations.is_empty(),
        "template is not minable: {}",
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

/// Poll `propose_txs` until its template satisfies `predicate`, surfacing
/// whatever it fails with meanwhile. Not `wait_for_local_mempool`, whose empty
/// set on a failed `propose_txs` satisfies every absence check here.
async fn wait_for_template<F>(
    setup: &TestSetup,
    mut predicate: F,
) -> anyhow::Result<Vec<BlockTemplateTransaction>>
where
    F: FnMut(&[Txid]) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let template = local_block_template(&setup.mempool_sync).await?;
        let txids: Vec<Txid> = template.iter().map(|t| t.txid).collect();
        if predicate(&txids) {
            return Ok(template);
        }
        let () = setup.task_errors.ensure_empty("template")?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "template never satisfied the predicate; it holds {txids:?}"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
