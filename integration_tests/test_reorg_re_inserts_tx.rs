//! A reorg restores a confirmed tx to the local mempool, and the sync task
//! stays healthy.

use std::time::Duration;

use bitcoin_jsonrpsee::{MainClient as _, client::GetBlockClient as _};

use crate::{setup::TestSetup, util::wait_for_mempool_pred};

pub async fn test_reorg_re_inserts_tx(setup: TestSetup) -> anyhow::Result<()> {
    let tx = setup.submit_and_wait(50_000).await?;
    let pre_reorg_tip = setup.node.rpc_client.getbestblockhash().await?;

    let mined = setup
        .node
        .rpc_client
        .generate_to_address(
            2,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?;
    let (b1, b2) = (mined[0], mined[1]);
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
    setup.wait_for_local_tip(b2, Duration::from_secs(5)).await?;

    setup.node.rpc_client.invalidate_block(b1).await?;

    wait_for_mempool_pred(
        &setup.node.rpc_client,
        Duration::from_secs(10),
        |t| t.contains(&tx),
        "tx back in bitcoind mempool",
    )
    .await?;
    setup
        .wait_for_local_tip(pre_reorg_tip, Duration::from_secs(10))
        .await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&tx),
            "tx re-inserted into local mempool",
        )
        .await?;

    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    anyhow::ensure!(
        setup.enforcer.disconnect_block_calls() >= 2,
        "expected ≥2 enforcer.disconnect_block calls"
    );
    Ok(())
}
