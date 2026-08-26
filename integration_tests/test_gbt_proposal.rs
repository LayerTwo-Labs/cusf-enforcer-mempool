//! BIP23 `getblocktemplate` proposal mode, against a live jsonrpsee server +
//! regtest bitcoind.
//!
//! `validate_proposal` consults two layers, in order: the node's own
//! consensus check, then the enforcer's `validate_block`. All three outcomes
//! are covered here -- a proposal that clears both layers, one the node
//! rejects, and one only the enforcer rejects.
//!
//! Runs over `MockEnforcer` rather than `DefaultEnforcer`, whose
//! `validate_block` accepts unconditionally and so leaves the
//! enforcer-rejection path unreachable.

use std::{net::SocketAddr, time::Duration};

use bitcoin::{
    Block, Network, Transaction, TxMerkleNode, block::Header,
    consensus::encode, hashes::Hash as _,
};
use bitcoin_jsonrpsee::{
    MainClient,
    client::{
        BlockTemplate, BlockTemplateRequest, CoinbaseTxnOrValue, MODE_PROPOSAL,
    },
    jsonrpsee,
};
use cusf_enforcer_mempool::server::{
    BlockTemplateResponse, RpcClient, RpcServer as _, Server,
};

use crate::{
    mock_enforcer::MockEnforcer,
    setup::{Directories, RegtestNode, start_mempool_sync},
    util::BinPaths,
};

/// Assemble the block a template describes: the server's own coinbase
/// followed by the template's transactions in order, under a header carrying
/// the template's version, prev hash, bits and time.
///
/// BIP23 proposals are validated without proof of work, so the nonce is left
/// at zero rather than mined.
fn block_from_template(template: &BlockTemplate) -> anyhow::Result<Block> {
    let CoinbaseTxnOrValue::Txn(coinbase) = &template.coinbase_txn_or_value
    else {
        anyhow::bail!(
            "template carried coinbasevalue; the `coinbasetxn` capability \
             should have produced a coinbase transaction"
        );
    };
    let mut txdata = vec![encode::deserialize::<Transaction>(&coinbase.data)?];
    for tx in &template.transactions {
        txdata.push(encode::deserialize::<Transaction>(&tx.data)?);
    }
    let mut block = Block {
        header: Header {
            version: template.version,
            prev_blockhash: template.prev_blockhash,
            // Computed below, once `txdata` is in place.
            merkle_root: TxMerkleNode::all_zeros(),
            // Floored at `mintime`: regtest primes 110 blocks in a burst,
            // which leaves the tip's median time past ahead of wall clock,
            // and `curtime` alone would be rejected as `time-too-old`.
            time: template.current_time.max(template.mintime) as u32,
            bits: template.compact_target,
            nonce: 0,
        },
        txdata,
    };
    block.header.merkle_root =
        block.compute_merkle_root().ok_or_else(|| {
            anyhow::anyhow!("a block with no txs has no merkle root")
        })?;
    Ok(block)
}

/// `getblocktemplate` in proposal mode, returning the BIP23 verdict: `None`
/// accepts, `Some(reason)` rejects.
async fn propose(
    client: &jsonrpsee::http_client::HttpClient,
    block: &Block,
) -> anyhow::Result<Option<String>> {
    let response = RpcClient::get_block_template(
        client,
        BlockTemplateRequest {
            mode: Some(MODE_PROPOSAL.into()),
            data: Some(encode::serialize_hex(block).into()),
            rules: vec!["segwit".to_owned()],
            ..Default::default()
        },
    )
    .await?;
    match response {
        BlockTemplateResponse::Proposal(verdict) => Ok(verdict),
        BlockTemplateResponse::Template(_) => {
            Err(anyhow::anyhow!("proposal mode answered with a template"))
        }
    }
}

pub async fn test_gbt_proposal(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;
    let enforcer = MockEnforcer::new();
    let (mempool_sync, _task_errors) =
        start_mempool_sync(&node, enforcer.clone(), None).await?;

    // Wire the GBT server the same way the demo app does.
    let network_info = node.rpc_client.get_network_info().await?;
    let sample_block_template = MainClient::get_block_template(
        &node.rpc_client,
        BlockTemplateRequest::default(),
    )
    .await?;
    let server = Server::new(
        node.mining_address.script_pubkey(),
        mempool_sync,
        Network::Regtest,
        network_info,
        node.rpc_client.clone(),
        None,
        sample_block_template,
    )?;
    let rpc_server = jsonrpsee::server::Server::builder()
        .build("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let rpc_addr = rpc_server.local_addr()?;
    // Dropping the handle stops the server — keep it alive for the test.
    let _rpc_server_handle = rpc_server.start(server.into_rpc());
    let client = jsonrpsee::http_client::HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(30))
        .build(format!("http://{rpc_addr}"))?;

    // `coinbasetxn` so the template carries a ready-made coinbase — that is
    // what makes the block assemblable here without re-deriving the witness
    // commitment.
    let template = RpcClient::get_block_template(
        &client,
        BlockTemplateRequest {
            capabilities: ["coinbasetxn".to_owned()].into_iter().collect(),
            rules: vec!["segwit".to_owned()],
            ..Default::default()
        },
    )
    .await?
    .into_template()
    .ok_or_else(|| anyhow::anyhow!("template mode answered with a verdict"))?;
    let block = block_from_template(&template)?;
    let block_hash = block.block_hash();

    // 1. A block built from the server's own template clears both layers.
    let verdict = propose(&client, &block).await?;
    anyhow::ensure!(
        verdict.is_none(),
        "a block built from the server's own template must be accepted, \
         got {verdict:?}"
    );
    anyhow::ensure!(
        enforcer.validate_block_calls().contains(&block_hash),
        "an accepted proposal must have reached the enforcer"
    );

    // 2. Rejected by the node. A corrupted merkle root must also never reach
    // the enforcer, since consensus is checked first.
    let mut bad_merkle_root = block.clone();
    bad_merkle_root.header.merkle_root = TxMerkleNode::all_zeros();
    let calls_before = enforcer.validate_block_calls().len();
    let verdict = propose(&client, &bad_merkle_root).await?;
    anyhow::ensure!(
        verdict.as_deref() == Some("bad-txnmrklroot"),
        "expected the node's `bad-txnmrklroot`, got {verdict:?}"
    );
    anyhow::ensure!(
        enforcer.validate_block_calls().len() == calls_before,
        "a block the node rejected must not be forwarded to the enforcer"
    );

    // 3. Rejected by the enforcer alone: the very block the node accepted in
    // step 1, now configured as invalid.
    enforcer.reject_block(block_hash);
    let verdict = propose(&client, &block).await?;
    anyhow::ensure!(
        verdict.as_deref()
            == Some("mock enforcer: block is configured as invalid"),
        "expected the enforcer's rejection reason, got {verdict:?}"
    );

    Ok(())
}
