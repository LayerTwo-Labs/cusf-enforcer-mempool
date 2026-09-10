//! A block that evicts a conflicting mempool tx must not read as a dropped
//! ZMQ message.
//!
//! Core bumps the mempool sequence for every removal but publishes nothing for
//! the ones a block confirms (`removeUnchecked`, suppressed for
//! `MemPoolRemovalReason::BLOCK`). A conflict eviction in the same
//! `removeForBlock` pass *is* published, so it arrives numbered past those
//! silent bumps — and before the block's own `C`, since `ConnectTip` removes
//! for the block before signalling it. `check_mempool_seq` is still in
//! `Equal` state at that point and treats the forward jump as a missing
//! message, killing the sequence stream over a gap that never existed.

use std::time::Duration;

use crate::{
    setup::TestSetup,
    util::{
        generate_block_entries, signed_spend_hex, spend_output,
        wait_for_mempool_pred, wallet_outputs_of,
    },
};

const FEE_SAT: u64 = 5_000;

pub async fn test_conflict_eviction_at_block_connect(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    // Confirm a funding tx first: the double spend below has to reach the
    // block without its parent, so that parent must already be on chain.
    let funder = setup.submit_and_wait(2_000_000).await?;
    let funding_block = generate_block_entries(
        rpc,
        &setup.node.mining_address,
        &[funder.to_string()],
    )
    .await?;
    setup
        .wait_for_local_tip(funding_block, Duration::from_secs(10))
        .await?;

    let (outpoint, value_sat) = wallet_outputs_of(rpc, funder)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("{funder} has no wallet output"))?;

    // `evicted` is in the mempool; `double_spend` takes the same outpoint and
    // is never broadcast, so only the block introduces it.
    let evicted = spend_output(rpc, outpoint, value_sat, FEE_SAT).await?;
    // An ordinary mempool tx, mined by the same block. Its removal is the
    // silent one that advances the sequence with nothing published, so the
    // eviction that follows arrives numbered past it.
    let mined = setup.submit_and_wait(1_000_000).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&evicted) && t.contains(&mined),
            "both txs in local mempool",
        )
        .await?;
    let double_spend =
        signed_spend_hex(rpc, outpoint, value_sat, FEE_SAT * 4).await?;

    // `mined` before `double_spend`: `removeForBlock` walks the block in
    // order, so the silent removal has to land before the eviction it makes
    // jump.
    let block = generate_block_entries(
        rpc,
        &setup.node.mining_address,
        &[mined.to_string(), double_spend],
    )
    .await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| !t.contains(&evicted) && !t.contains(&mined),
        "conflict evicted from bitcoind mempool",
    )
    .await?;

    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| !t.contains(&evicted) && !t.contains(&mined),
            "conflict and mined tx out of the local mempool",
        )
        .await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
