//! A tx can be valid in isolation, yet invalid in the context of a block
//! proposal. Tts validity can depend on state that an earlier tx in the
//! proposal modifies. Such a tx is accepted into the mempool, so without 
//! proposal validation it ends up in every block proposal, every `getblocktemplate` 
//! call fails, and block production is blocked.
//!
//! [`CusfBlockProducer::validate_block_proposal`] lets the block producer
//! identify the offending tx, so that the server excludes it (and its
//! descendants) from the proposal and serves a valid template instead.

use std::{collections::HashSet, time::Duration};

use anyhow::{Context as _, anyhow};
use bitcoin::{Address, Txid, hashes::Hash as _};
use bitcoin_jsonrpsee::{
    MainClient as _,
    client::BlockTemplateRequest,
    jsonrpsee::{core::client::ClientT, rpc_params},
};
use cusf_enforcer_mempool::server::{RpcServer as _, Server};

use crate::{
    mock_block_producer::MockBlockProducer,
    setup::{
        Directories, RegtestNode, start_mempool_sync, wait_for_local_mempool,
    },
    util::{BinPaths, RpcClient, get_new_address, send_to_address, submit_tx},
};

const WAIT: Duration = Duration::from_secs(15);

/// Vout of `txid` paying to `addr`
async fn find_vout_paying(
    rpc: &RpcClient,
    txid: Txid,
    addr: &Address,
) -> anyhow::Result<u32> {
    let tx: serde_json::Value = rpc
        .request("getrawtransaction", rpc_params![txid.to_string(), true])
        .await?;
    let addr = addr.to_string();
    tx["vout"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|vout| {
            if vout["scriptPubKey"]["address"].as_str() == Some(&addr) {
                vout["n"].as_u64().map(|n| n as u32)
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow!("no output of `{txid}` pays to `{addr}`"))
}

/// Spend `parent_txid:parent_vout` to a fresh wallet address via the raw tx
/// RPCs, deterministically chaining a child on an unconfirmed parent
async fn send_child_spending(
    rpc: &RpcClient,
    parent_txid: Txid,
    parent_vout: u32,
    output_sat: u64,
) -> anyhow::Result<Txid> {
    let dest = get_new_address(rpc).await?;
    let inputs = serde_json::json!([{
        "txid": parent_txid.to_string(),
        "vout": parent_vout,
    }]);
    let amount_btc = format!("{:.8}", (output_sat as f64) / 100_000_000.0);
    let outputs = serde_json::json!({ dest.to_string(): amount_btc });
    let raw: String = rpc
        .request("createrawtransaction", rpc_params![inputs, outputs])
        .await?;
    #[derive(serde::Deserialize)]
    struct Signed {
        hex: String,
        complete: bool,
    }
    let signed: Signed = rpc
        .request("signrawtransactionwithwallet", rpc_params![raw])
        .await?;
    anyhow::ensure!(signed.complete, "signing child tx incomplete");
    let txid: String = rpc
        .request("sendrawtransaction", rpc_params![signed.hex])
        .await?;
    Ok(txid.parse()?)
}

/// Txids included in a `getblocktemplate` response from `server`
async fn template_txids(
    server: &Server<MockBlockProducer, RpcClient>,
) -> anyhow::Result<HashSet<Txid>> {
    let template = server
        .get_block_template(BlockTemplateRequest::default())
        .await
        .context("getblocktemplate")?;
    Ok(template.transactions.iter().map(|tx| tx.txid).collect())
}

pub async fn test_validate_block_proposal(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;
    let block_producer = MockBlockProducer::new();
    let (mempool_sync, task_errors) =
        start_mempool_sync(&node, block_producer.clone()).await?;

    // A parent with a child chained on it, plus an unrelated tx
    let parent_dest = get_new_address(&node.rpc_client).await?;
    let parent =
        send_to_address(&node.rpc_client, &parent_dest, 100_000).await?;
    let parent_vout =
        find_vout_paying(&node.rpc_client, parent, &parent_dest).await?;
    let child =
        send_child_spending(&node.rpc_client, parent, parent_vout, 80_000)
            .await?;
    let unrelated = submit_tx(&node.rpc_client, 50_000).await?;
    tracing::info!(%parent, %child, %unrelated, "submitted txs");
    let () = wait_for_local_mempool(
        &mempool_sync,
        &task_errors,
        WAIT,
        |txids| {
            [parent, child, unrelated]
                .iter()
                .all(|txid| txids.contains(txid))
        },
        "all txs in local mempool",
    )
    .await?;

    let network_info = node.rpc_client.get_network_info().await?;
    let sample_block_template = node
        .rpc_client
        .get_block_template(BlockTemplateRequest::default())
        .await
        .context("sample getblocktemplate")?;
    let server = Server::new(
        node.mining_address.script_pubkey(),
        mempool_sync,
        bitcoin::Network::Regtest,
        network_info,
        node.rpc_client.clone(),
        None,
        sample_block_template,
    )?;

    // Control: all txs are proposed
    let txids = template_txids(&server).await?;
    for txid in [parent, child, unrelated] {
        anyhow::ensure!(
            txids.contains(&txid),
            "control template missing `{txid}`"
        );
    }

    // The parent is now invalid in the context of any proposal containing
    // it. `getblocktemplate` must still succeed — with the parent and its
    // child excluded — instead of failing on every call.
    block_producer.set_invalid_proposal_txid(parent);
    let txids = template_txids(&server).await?;
    anyhow::ensure!(
        !txids.contains(&parent),
        "template contains invalid tx `{parent}`"
    );
    anyhow::ensure!(
        !txids.contains(&child),
        "template contains descendant `{child}` of invalid tx"
    );
    anyhow::ensure!(
        txids.contains(&unrelated),
        "template missing valid tx `{unrelated}`"
    );
    // The suffix hook only ever sees validated proposals
    let suffix_proposals = block_producer.suffix_proposals();
    let last_suffix_proposal = suffix_proposals
        .last()
        .ok_or_else(|| anyhow!("no suffix proposals recorded"))?;
    anyhow::ensure!(
        !last_suffix_proposal.contains(&parent)
            && !last_suffix_proposal.contains(&child),
        "`block_template_suffix` saw an unvalidated proposal: \
         {last_suffix_proposal:?}"
    );

    // The mempool is not mutated: clearing the verdict restores the txs
    block_producer.clear_invalid_proposal_txids();
    let txids = template_txids(&server).await?;
    for txid in [parent, child, unrelated] {
        anyhow::ensure!(
            txids.contains(&txid),
            "template missing `{txid}` after clearing verdicts"
        );
    }

    // An invalid tx that cannot be excluded from the proposal (not a
    // proposal tx) must fail template generation, not loop forever
    let absent_txid = Txid::from_byte_array([0xAB; 32]);
    block_producer.set_force_invalid_txid(Some(absent_txid));
    anyhow::ensure!(
        template_txids(&server).await.is_err(),
        "getblocktemplate should fail when the invalid tx cannot be excluded"
    );
    block_producer.set_force_invalid_txid(None);

    let () = task_errors.ensure_empty("end of test")?;
    Ok(())
}
