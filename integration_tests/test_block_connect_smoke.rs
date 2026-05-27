//! Smoke test for the block-connect path: mining advances the local tip,
//! confirmed txs are dropped from the local mempool view, and the sync task
//! keeps processing afterwards. Doesn't gate any specific PR17 finding;
//! catches gross regressions in `connect_block`.

use std::time::Duration;

use bitcoin_jsonrpsee::{MainClient as _, client::GetBlockClient as _};

use crate::setup::TestSetup;

pub async fn test_block_connect_smoke(setup: TestSetup) -> anyhow::Result<()> {
    let tx = setup.submit_and_wait(50_000).await?;

    let block_hash = *setup
        .node
        .rpc_client
        .generate_to_address(
            1,
            &setup.node.mining_address.clone().into_unchecked(),
        )
        .await?
        .last()
        .unwrap();
    let block = setup
        .node
        .rpc_client
        .get_block(block_hash, bitcoin_jsonrpsee::client::U8Witness::<2>)
        .await?;
    anyhow::ensure!(
        block.tx.iter().any(|t| t.txid == tx),
        "expected {tx} in block {block_hash}"
    );
    setup
        .wait_for_local_tip(block_hash, Duration::from_secs(5))
        .await?;

    let _followup = setup.submit_and_wait(40_000).await?;
    anyhow::ensure!(!setup.local_mempool_txids().await.contains(&tx));
    Ok(())
}
