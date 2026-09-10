//! The disconnect twin of the connect-side removal: a tx the enforcer removes
//! at block disconnect, and every descendant that goes with it, must be
//! recorded as rejected.
//!
//! `handle_disconnected_block` runs the enforcer's `remove_mempool_txs`
//! through `remove_with_descendants` and marks nothing at all — not even the
//! txid the enforcer named, which the connect path has always marked. Those
//! txs stay in the node's mempool and in `tx_cache`, so a later child resolves
//! one straight out of the cache and is admitted with nothing to depend on.

use std::{collections::HashSet, time::Duration};

use bitcoin::Txid;
use bitcoin_jsonrpsee::{MainClient as _, client::BlockTemplateTransaction};

use crate::{
    setup::{TestSetup, local_block_template},
    util::{
        TemplateViolation, generate_block, get_new_address, submit_child_of,
        template_violations, wait_for_mempool_pred,
    },
};

const CHAINED_FEE_SAT: u64 = 5_000;

pub async fn test_orphan_admitted_after_disconnect_removal(
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

    // An empty block, so the package stays unconfirmed and is still there to
    // be removed when the block is rolled back.
    setup
        .enforcer
        .set_always_remove_on_disconnect(HashSet::from([parent]));
    let block = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;

    let pre_reorg = rpc.getbestblockhash().await?;
    anyhow::ensure!(
        pre_reorg == block,
        "expected tip {block}, got {pre_reorg}"
    );
    rpc.invalidate_block(block).await?;
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

    let grandchild = submit_child_of(rpc, child, CHAINED_FEE_SAT).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&grandchild),
        "grandchild in bitcoind mempool",
    )
    .await?;

    // Barrier: this block's connect action is queued behind the grandchild's,
    // so reaching it locally means the grandchild has been decided. Mined to a
    // fresh address, or it would be byte-identical to the invalidated block
    // and rejected as `duplicate-invalid`.
    let barrier_addr = get_new_address(rpc).await?;
    let barrier = generate_block(rpc, &barrier_addr, &[]).await?;
    setup
        .wait_for_local_tip(barrier, Duration::from_secs(10))
        .await?;

    let template = local_block_template(&setup.mempool_sync).await?;
    let txids: Vec<Txid> = template.iter().map(|t| t.txid).collect();
    anyhow::ensure!(
        !txids.contains(&grandchild),
        "grandchild {grandchild} was admitted though its parent {child} was \
         removed with {parent} at disconnect; template holds {txids:?}"
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
