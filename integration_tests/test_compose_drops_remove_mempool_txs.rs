//! Composing enforcers with mixed-results must propagate the
//! rolled-back side's `remove_mempool_txs`.

use std::{collections::HashSet, time::Duration};

use cusf_enforcer_mempool::cusf_enforcer::Compose;
use tokio::time::sleep;

use crate::{
    mock_enforcer::MockEnforcer,
    setup::{
        Directories, RegtestNode, local_mempool_txids, start_mempool_sync,
        wait_for_local_mempool,
    },
    util::{
        BinPaths, generate_block, get_new_address, submit_tx,
        wait_for_mempool_pred,
    },
};

pub async fn test_compose_drops_remove_mempool_txs(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    // Subject tx: both Compose halves default-accept it into mempool.
    let tx = submit_tx(&node.rpc_client, 50_000).await?;
    wait_for_mempool_pred(
        &node.rpc_client,
        Duration::from_secs(5),
        |t| t.contains(&tx),
        "subject tx in bitcoind mempool",
    )
    .await?;

    let enforcer_a = MockEnforcer::new();
    let enforcer_b = MockEnforcer::new();
    let compose = Compose::new(enforcer_a.clone(), enforcer_b.clone());
    let (mempool_sync, task_errors) =
        start_mempool_sync(&node, compose).await?;

    wait_for_local_mempool(
        &mempool_sync,
        &task_errors,
        Duration::from_secs(5),
        |t| t.contains(&tx),
        "subject tx in local mempool",
    )
    .await?;

    // Trigger the mixed-result branch: A accepts (and on disconnect demands
    // the subject tx be removed), B rejects every block.
    enforcer_a.set_always_remove_on_disconnect(HashSet::from([tx]));
    enforcer_b.set_reject_all_blocks(true);

    let dest = get_new_address(&node.rpc_client).await?;
    generate_block(&node.rpc_client, &dest, &[]).await?;
    sleep(Duration::from_secs(2)).await;

    anyhow::ensure!(
        task_errors.is_empty(),
        "task errors: {:?}",
        task_errors.snapshot()
    );
    anyhow::ensure!(
        enforcer_a.disconnect_block_calls() >= 1,
        "expected A.disconnect_block to fire (Compose's rollback path)"
    );

    let local = local_mempool_txids(&mempool_sync).await;
    anyhow::ensure!(
        !local.contains(&tx),
        "Compose's mixed-result branch dropped A.disconnect_block's \
         remove_mempool_txs: subject tx still in local mempool: {local:?}"
    );
    Ok(())
}
