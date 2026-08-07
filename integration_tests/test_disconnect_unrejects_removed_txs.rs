//! The mirror image of `test_connect_block_deprioritizes_removed_txs`: what a
//! connected block does to a tx it invalidates has to be undone when that
//! block is disconnected.
//!
//! Rejecting a tx does two things, neither of which reverses itself. The tx is
//! dropped from our mempool mirror while bitcoind keeps it, so no announcement
//! will ever bring it back; and `prioritisetransaction` applies a delta that
//! accumulates and outlives the mempool entry. A reorg can make the tx valid
//! again -- the block it lost to is gone -- and yet leave it invisible to the
//! mirror and unmineable on the node.
//!
//! Concretely, for the BMM bid this exists to handle: the bid loses its
//! auction to a block, that block is reorged away, and the bid is once more a
//! valid request for the tip. Nothing should have to re-place it.

use std::{collections::HashSet, time::Duration};

use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::TestSetup,
    util::{generate_block, modified_fee_sat},
};

pub async fn test_disconnect_unrejects_removed_txs(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    let tx = setup.submit_and_wait(50_000).await?;
    let baseline = modified_fee_sat(rpc, tx).await?;
    anyhow::ensure!(
        baseline > 0,
        "expected {tx} to start with a positive modified fee, got {baseline} sat"
    );
    let pre_reorg_tip = rpc.getbestblockhash().await?;

    // A block that invalidates the tx without mining it, so bitcoind keeps it
    // and only the enforcer considers it dead.
    setup
        .enforcer
        .set_always_remove_on_connect(HashSet::from([tx]));
    let block = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;

    // Both halves of the rejection have to have happened, or there is nothing
    // to undo below and this test would pass without exercising anything.
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |txids| !txids.contains(&tx),
            "removed tx gone from the local mempool mirror",
        )
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if modified_fee_sat(rpc, tx).await? < 0 {
            break;
        }
        let () = setup.task_errors.ensure_empty("deprioritisation")?;
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "expected {tx} to be deprioritized by the connected block first"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }

    // The block goes away, and with it the reason the tx was rejected.
    setup.enforcer.set_always_remove_on_connect(HashSet::new());
    let () = rpc.invalidate_block(block).await?;
    setup
        .wait_for_local_tip(pre_reorg_tip, Duration::from_secs(10))
        .await?;

    // The node's own view: no template can carry the tx while the delta
    // stands, whoever builds it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let restored = loop {
        let modified = modified_fee_sat(rpc, tx).await?;
        if modified >= 0 {
            break modified;
        }
        let () = setup.task_errors.ensure_empty("undoing deprioritisation")?;
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "{tx} is a valid mempool tx again -- the block that removed it \
                 was disconnected -- but its modified fee is still {modified} \
                 sat (baseline {baseline} sat). The rejection's fee delta is \
                 never undone, so no template will select it again."
            );
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    };
    anyhow::ensure!(
        restored == baseline,
        "expected {tx} to be restored to its own fee of {baseline} sat, got \
         {restored} sat"
    );

    // Our own view: bitcoind never dropped the tx, so nothing announces it,
    // and only an explicit re-fetch puts it back in the mirror.
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |txids| txids.contains(&tx),
            "unrejected tx back in the local mempool mirror",
        )
        .await?;

    let () = setup.task_errors.ensure_empty("end of test")?;
    Ok(())
}
