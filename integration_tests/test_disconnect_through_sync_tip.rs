//! A reorg that disconnects the initial-sync tip must leave `Mempool::tip()`
//! usable. `init_sync_mempool` fetches only the sync-time tip into
//! `chain.blocks` — never its parent — so when `handle_disconnected_block`
//! rolls `chain.tip` back to the parent hash, `Mempool::tip()`
//! (`chain.blocks[&chain.tip]`) panics on the missing key until the next
//! block connects, killing e.g. `getblocktemplate` callers.

use std::time::Duration;

use anyhow::anyhow;
use bitcoin::BlockHash;
use bitcoin_jsonrpsee::{
    MainClient as _,
    jsonrpsee::{core::client::ClientT, rpc_params},
};

use crate::{
    setup::{Directories, RegtestNode, TestSetup},
    util::{BinPaths, RpcClient},
};

async fn get_block_parent(
    rpc: &RpcClient,
    hash: BlockHash,
) -> anyhow::Result<BlockHash> {
    let res: serde_json::Value = rpc
        .request("getblockheader", rpc_params![hash.to_string()])
        .await?;
    let prev = res["previousblockhash"].as_str().ok_or_else(|| {
        anyhow!("getblockheader missing `previousblockhash`: {res:?}")
    })?;
    Ok(prev.parse()?)
}

pub async fn test_disconnect_through_sync_tip(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    // Let bitcoind finish flushing ZMQ connect events from the priming mine
    // before init-sync subscribes. A stale connect replay fetches the sync
    // tip's ancestors, masking the missing-parent case
    // this test exercises.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let setup = TestSetup::start(node).await?;
    let sync_tip = setup.node.rpc_client.getbestblockhash().await?;
    let parent = get_block_parent(&setup.node.rpc_client, sync_tip).await?;

    setup.node.rpc_client.invalidate_block(sync_tip).await?;

    setup
        .wait_for_local_tip(parent, Duration::from_secs(15))
        .await?;

    anyhow::ensure!(
        setup.enforcer.disconnect_block_calls() == 1,
        "expected exactly one disconnect_block call, got {}",
        setup.enforcer.disconnect_block_calls()
    );
    Ok(())
}
