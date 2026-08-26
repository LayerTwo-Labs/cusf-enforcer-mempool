//! BIP22 long-poll integration coverage for the GBT server, against a live
//! jsonrpsee server + regtest bitcoind. Three request shapes:
//! - no `longpollid` → responds immediately, carrying a `longpollid`
//! - stale `longpollid` → responds immediately with the current template
//! - current `longpollid` → parks until a block connects, then responds
//!   with a template for the new tip
//!
//! Runs over `DefaultEnforcer` (not `MockEnforcer`): the GBT server bounds
//! its enforcer by `CusfBlockProducer`, which the mock doesn't implement.

use std::{net::SocketAddr, time::Duration};

use bitcoin::{BlockHash, Network, hashes::Hash as _};
use bitcoin_jsonrpsee::{
    MainClient,
    client::{BlockTemplate, BlockTemplateRequest},
    jsonrpsee,
};
use cusf_enforcer_mempool::{
    cusf_enforcer::DefaultEnforcer,
    // Both `MainClient` and the generated `RpcClient` define
    // `get_block_template`, so calls below use fully qualified syntax.
    server::{RpcClient, RpcServer as _, Server},
};

use crate::{
    setup::{Directories, RegtestNode, start_mempool_sync},
    util::BinPaths,
};

/// `getblocktemplate` in template mode, unwrapping the response variant.
/// `mode` is left unset, so this exercises the default-is-template path.
async fn get_template(
    client: &jsonrpsee::http_client::HttpClient,
    request: BlockTemplateRequest,
) -> anyhow::Result<BlockTemplate> {
    RpcClient::get_block_template(client, request)
        .await?
        .into_template()
        .map(|template| *template)
        .ok_or_else(|| {
            anyhow::anyhow!("expected a template, got a proposal verdict")
        })
}

/// Upper bound for a request that must NOT long poll. Generous for slow CI,
/// but far under the server's 30s long-poll window, so a request that
/// wrongly parks still fails the assertion.
const IMMEDIATE: Duration = Duration::from_secs(10);

pub async fn test_gbt_long_poll(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;
    let (mempool_sync, task_errors) =
        start_mempool_sync(&node, DefaultEnforcer, None).await?;

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
        // Must exceed the server's long-poll window (30s).
        .request_timeout(Duration::from_secs(90))
        .build(format!("http://{rpc_addr}"))?;

    // 1. No longpollid: immediate response that advertises long polling.
    let template = tokio::time::timeout(
        IMMEDIATE,
        get_template(&client, BlockTemplateRequest::default()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("plain GBT did not respond immediately"))??;
    let tip = template.prev_blockhash;
    let first_long_poll_id = template
        .long_poll_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("template without longpollid"))?;
    anyhow::ensure!(
        first_long_poll_id.starts_with(&tip.to_string()),
        "expected longpollid prefixed by tip ({tip}), got {first_long_poll_id}"
    );

    // 2. Stale longpollid: server must not park a request for a template
    // that is already outdated.
    let template = tokio::time::timeout(
        IMMEDIATE,
        get_template(
            &client,
            BlockTemplateRequest {
                long_poll_id: Some(BlockHash::all_zeros().to_string()),
                ..Default::default()
            },
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("stale-longpollid GBT did not respond immediately")
    })??;
    anyhow::ensure!(
        template.prev_blockhash == tip,
        "stale long poll should serve the current tip ({tip}), got {}",
        template.prev_blockhash
    );
    // BIP22: the longpollid MUST be unique for each event — a same-tip
    // rebuild still gets a fresh id.
    anyhow::ensure!(
        template.long_poll_id.as_ref().is_some_and(|long_poll_id| {
            long_poll_id.starts_with(&tip.to_string())
                && *long_poll_id != first_long_poll_id
        }),
        "same-tip rebuild must carry a fresh longpollid, got {:?} after {}",
        template.long_poll_id,
        first_long_poll_id
    );

    // 3. Current longpollid: the request parks...
    let parked = tokio::spawn({
        let client = client.clone();
        let long_poll_id = Some(first_long_poll_id);
        async move {
            get_template(
                &client,
                BlockTemplateRequest {
                    long_poll_id,
                    ..Default::default()
                },
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    anyhow::ensure!(
        !parked.is_finished(),
        "long poll for the current tip returned without a tip change"
    );

    // ...until a block connects, and then serves the new tip promptly.
    let new_tip = *node
        .rpc_client
        .generate_to_address(1, &node.mining_address.clone().into_unchecked())
        .await?
        .last()
        .unwrap();
    let template = tokio::time::timeout(Duration::from_secs(20), parked)
        .await
        .map_err(|_| {
            anyhow::anyhow!("long poll did not wake on the new block")
        })???;
    anyhow::ensure!(
        template.prev_blockhash == new_tip,
        "woken long poll should serve the new tip ({new_tip}), got {}",
        template.prev_blockhash
    );
    anyhow::ensure!(
        template.long_poll_id.as_ref().is_some_and(
            |long_poll_id| long_poll_id.starts_with(&new_tip.to_string())
        ),
        "woken long poll should carry a new-tip longpollid, got {:?}",
        template.long_poll_id
    );

    task_errors.ensure_empty("gbt long poll")?;
    Ok(())
}
