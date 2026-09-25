//! Slipstream txs against a live regtest bitcoind and GBT server.
//!
//! A slipstream tx is never in the node's mempool, so everything the sync task
//! normally learns from the node -- conflicts, evictions, a parent leaving --
//! has to be worked out for it here. Each phase checks one of those, and every
//! template is mined with `generateblock`, which only succeeds if the node
//! finds the whole block valid.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context as _, anyhow};
use bitcoin::{
    Amount, BlockHash, Network, OutPoint, ScriptBuf, Sequence, Transaction,
    TxIn, TxOut, Txid, Witness, absolute::LockTime, hashes::Hash as _,
    hex::DisplayHex as _, transaction::Version,
};
use bitcoin_jsonrpsee::{
    MainClient,
    client::{BlockTemplateRequest, BlockTemplateTransaction},
    jsonrpsee::{
        self,
        core::client::ClientT as _,
        http_client::{HttpClient, HttpClientBuilder},
        rpc_params,
    },
};
use cusf_enforcer_mempool::{
    cusf_enforcer::DefaultEnforcer,
    mempool::{SlipstreamRemoval, SlipstreamTxStatus},
    server::{
        RpcClient as _, RpcServer as _, Server, SlipstreamConfig,
        SubmitSlipstreamTxResponse,
    },
};
use tokio::time::sleep;

use crate::{
    setup::{Directories, RegtestNode, TaskErrors, start_mempool_sync},
    util::{
        BinPaths, RpcClient, bump_fee, generate_block, generate_block_entries,
        get_new_address, mempool_txids, signed_spend_hex, spend_output,
        submit_tx, template_violations, wallet_outputs_of,
    },
};

const FUNDING_SAT: u64 = 2_000_000;

struct Harness {
    node: RegtestNode,
    /// Client for the GBT server under test
    server: HttpClient,
    task_errors: TaskErrors,
    _server_handle: jsonrpsee::server::ServerHandle,
}

impl Harness {
    async fn new(
        bin_paths: &BinPaths,
        directories: Directories,
    ) -> anyhow::Result<Self> {
        let node = RegtestNode::new(bin_paths, directories).await?;
        let (mempool_sync, task_errors) =
            start_mempool_sync(&node, DefaultEnforcer, None).await?;
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
        )?
        .with_slipstream(SlipstreamConfig::default());
        let rpc_server = jsonrpsee::server::Server::builder()
            .build("127.0.0.1:0".parse::<SocketAddr>()?)
            .await?;
        let rpc_addr = rpc_server.local_addr()?;
        let server_handle = rpc_server.start(server.into_rpc());
        let server = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(60))
            .build(format!("http://{rpc_addr}"))?;
        Ok(Self {
            node,
            server,
            task_errors,
            _server_handle: server_handle,
        })
    }

    fn rpc(&self) -> &RpcClient {
        &self.node.rpc_client
    }

    /// A confirmed wallet output worth [`FUNDING_SAT`]
    async fn fund(&self) -> anyhow::Result<(OutPoint, u64)> {
        let funder = submit_tx(self.rpc(), FUNDING_SAT).await?;
        let block =
            generate_block(self.rpc(), &self.node.mining_address, &[funder])
                .await?;
        self.wait_for_tip(block).await?;
        let (outpoint, value) = wallet_outputs_of(self.rpc(), funder)
            .await?
            .into_iter()
            .find(|(_, value)| *value == FUNDING_SAT)
            .ok_or_else(|| {
                anyhow!("{funder} has no {FUNDING_SAT} sat output")
            })?;
        // The wallet never learns of a slipstream spend, so it would pick
        // this output to fund something else. Locked, it only signs for it.
        let locked: bool = self
            .rpc()
            .request(
                "lockunspent",
                rpc_params![
                    false,
                    [serde_json::json!({
                        "txid": outpoint.txid.to_string(),
                        "vout": outpoint.vout,
                    })]
                ],
            )
            .await?;
        anyhow::ensure!(locked, "could not lock {outpoint}");
        Ok((outpoint, value))
    }

    async fn submit(
        &self,
        tx_hex: &str,
    ) -> anyhow::Result<SubmitSlipstreamTxResponse> {
        Ok(self.server.submit_slipstream_tx(tx_hex.to_owned()).await?)
    }

    async fn submit_accepted(
        &self,
        tx_hex: &str,
    ) -> anyhow::Result<SubmitSlipstreamTxResponse> {
        let response = self.submit(tx_hex).await?;
        anyhow::ensure!(
            response.accepted,
            "slipstream tx {} refused: {:?}",
            response.txid,
            response.reject_reason
        );
        Ok(response)
    }

    async fn status(&self, txid: Txid) -> anyhow::Result<SlipstreamTxStatus> {
        Ok(self.server.get_slipstream_tx(txid).await?)
    }

    async fn template(&self) -> anyhow::Result<Vec<BlockTemplateTransaction>> {
        let template =
            cusf_enforcer_mempool::server::RpcClient::get_block_template(
                &self.server,
                BlockTemplateRequest::default(),
            )
            .await?
            .into_template()
            .ok_or_else(|| {
                anyhow!("expected a template, got a proposal verdict")
            })?;
        let violations =
            template_violations(self.rpc(), &template.transactions).await?;
        anyhow::ensure!(
            violations.is_empty(),
            "template violations: {violations:?}"
        );
        Ok(template.transactions)
    }

    async fn template_txids(&self) -> anyhow::Result<Vec<Txid>> {
        Ok(self
            .template()
            .await?
            .into_iter()
            .map(|tx| tx.txid)
            .collect())
    }

    /// Mine exactly the current template. `generateblock` refuses a block the
    /// node finds invalid, so this also proves the template valid.
    async fn mine_template(&self) -> anyhow::Result<BlockHash> {
        let entries: Vec<String> = self
            .template()
            .await?
            .iter()
            .map(|tx| tx.data.to_lower_hex_string())
            .collect();
        let block = generate_block_entries(
            self.rpc(),
            &self.node.mining_address,
            &entries,
        )
        .await
        .context("mining the template")?;
        self.wait_for_tip(block).await?;
        Ok(block)
    }

    async fn wait_for_tip(&self, block: BlockHash) -> anyhow::Result<()> {
        self.wait_until("local tip", || async {
            let template =
                cusf_enforcer_mempool::server::RpcClient::get_block_template(
                    &self.server,
                    BlockTemplateRequest::default(),
                )
                .await?
                .into_template()
                .ok_or_else(|| anyhow!("expected a template"))?;
            Ok(template.prev_blockhash == block)
        })
        .await
    }

    async fn wait_for_status<F>(
        &self,
        txid: Txid,
        label: &str,
        mut predicate: F,
    ) -> anyhow::Result<SlipstreamTxStatus>
    where
        F: FnMut(&SlipstreamTxStatus) -> bool,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let status = self.status(txid).await?;
            if predicate(&status) {
                return Ok(status);
            }
            self.task_errors.ensure_empty(label)?;
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for {label}; last: {status:?}"
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_until<F, Fut>(
        &self,
        label: &str,
        mut f: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<bool>>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if f().await? {
                return Ok(());
            }
            self.task_errors.ensure_empty(label)?;
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for {label}");
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
}

fn decode(tx_hex: &str) -> anyhow::Result<Transaction> {
    Ok(bitcoin::consensus::encode::deserialize_hex(tx_hex)?)
}

fn encode(tx: &Transaction) -> String {
    bitcoin::consensus::encode::serialize_hex(tx)
}

async fn sign(
    rpc: &RpcClient,
    tx: &Transaction,
    prevtxs: serde_json::Value,
) -> anyhow::Result<String> {
    #[derive(serde::Deserialize)]
    struct Signed {
        hex: String,
        complete: bool,
    }
    let signed: Signed = rpc
        .request(
            "signrawtransactionwithwallet",
            rpc_params![encode(tx), prevtxs],
        )
        .await?;
    anyhow::ensure!(signed.complete, "could not sign {}", tx.compute_txid());
    Ok(signed.hex)
}

/// Fee under the node's relay minimum for a one-in, one-out spend of about
/// 110 vB. The minimum is 0.1 sat/vB since Core 30, so 11 sat.
const BELOW_RELAY_FEE_SAT: u64 = 5;

/// A spend of `outpoint` that the node will not take into its mempool: it
/// pays [`BELOW_RELAY_FEE_SAT`], under the minimum relay fee. That holds
/// whatever policy flags the node runs with, where standardness rules do not
/// (the test node runs `-acceptnonstdtxn`). It is otherwise ordinary, and
/// consensus-valid.
async fn unrelayable_spend_hex(
    rpc: &RpcClient,
    outpoint: OutPoint,
    value_sat: u64,
) -> anyhow::Result<String> {
    let pay_to = get_new_address(rpc).await?;
    let tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: outpoint,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(value_sat - BELOW_RELAY_FEE_SAT),
            script_pubkey: pay_to.script_pubkey(),
        }],
    };
    sign(rpc, &tx, serde_json::json!([])).await
}

/// A spend of output 0 of `parent`, which is in neither the chain nor the
/// node's mempool, so the wallet is told what it is spending.
async fn child_of_unrelayed_hex(
    rpc: &RpcClient,
    parent: &Transaction,
    fee_sat: u64,
) -> anyhow::Result<String> {
    let parent_out = &parent.output[0];
    let dest = get_new_address(rpc).await?;
    let tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::new(parent.compute_txid(), 0),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: parent_out.value - Amount::from_sat(fee_sat),
            script_pubkey: dest.script_pubkey(),
        }],
    };
    let prevtxs = serde_json::json!([{
        "txid": parent.compute_txid().to_string(),
        "vout": 0,
        "scriptPubKey": parent_out.script_pubkey.to_hex_string(),
        "amount": parent_out.value.to_btc(),
    }]);
    sign(rpc, &tx, prevtxs).await
}

async fn node_would_relay(
    rpc: &RpcClient,
    tx_hex: &str,
) -> anyhow::Result<bool> {
    let res: serde_json::Value = rpc
        .request("testmempoolaccept", rpc_params![[tx_hex]])
        .await?;
    res[0]["allowed"]
        .as_bool()
        .ok_or_else(|| anyhow!("unexpected testmempoolaccept result: {res}"))
}

fn ensure_rejected(
    response: &SubmitSlipstreamTxResponse,
    reason_contains: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !response.accepted
            && response
                .reject_reason
                .as_deref()
                .is_some_and(|reason| reason.contains(reason_contains)),
        "expected a rejection for `{reason_contains}`, got {response:?}"
    );
    Ok(())
}

/// A tx the node never relays reaches the template, and leaves the pool as
/// mined once the template is.
async fn mined_without_relay(h: &Harness) -> anyhow::Result<()> {
    let (outpoint, value) = h.fund().await?;
    let tx_hex = unrelayable_spend_hex(h.rpc(), outpoint, value).await?;
    anyhow::ensure!(
        !node_would_relay(h.rpc(), &tx_hex).await?,
        "fixture tx is supposed to be refused by the node"
    );

    let response = h.submit_accepted(&tx_hex).await?;
    let txid = response.txid;
    anyhow::ensure!(
        response.fee_sat == Some(BELOW_RELAY_FEE_SAT)
            && !response.already_present,
        "unexpected response: {response:?}"
    );
    let again = h.submit_accepted(&tx_hex).await?;
    anyhow::ensure!(again.already_present, "resubmit: {again:?}");

    anyhow::ensure!(
        !mempool_txids(h.rpc()).await?.contains(&txid),
        "slipstream tx reached the node's mempool"
    );
    anyhow::ensure!(
        matches!(h.status(txid).await?, SlipstreamTxStatus::Pending(_)),
        "not pending"
    );
    anyhow::ensure!(
        h.template_txids().await?.contains(&txid),
        "slipstream tx missing from the template"
    );

    let block = h.mine_template().await?;
    h.wait_for_status(txid, "mined", |status| {
        *status
            == SlipstreamTxStatus::Removed {
                txid,
                removal: SlipstreamRemoval::Mined { block_hash: block },
            }
    })
    .await?;
    Ok(())
}

/// Invalid txs are refused before they can reach a template.
async fn invalid_refused(h: &Harness) -> anyhow::Result<()> {
    let (outpoint, value) = h.fund().await?;

    // A signature that does not verify
    let mut bad_sig =
        decode(&signed_spend_hex(h.rpc(), outpoint, value, 5_000).await?)?;
    let mut witness: Vec<Vec<u8>> = bad_sig.input[0].witness.to_vec();
    let last = witness[0].len() - 2;
    witness[0][last] ^= 0x01;
    bad_sig.input[0].witness = Witness::from_slice(&witness);
    ensure_rejected(
        &h.submit(&encode(&bad_sig)).await?,
        "script-verify-flag-failed",
    )?;

    // An input no one has ever created
    let mut missing = bad_sig.clone();
    missing.input[0].previous_output =
        OutPoint::new(Txid::from_byte_array([0x11; 32]), 0);
    ensure_rejected(&h.submit(&encode(&missing)).await?, "missing-inputs")?;

    // Paying out more than it spends
    let mut overspend = bad_sig.clone();
    overspend.output[0].value = Amount::from_sat(value + 1);
    ensure_rejected(&h.submit(&encode(&overspend)).await?, "in-belowout")?;

    // An input the chain has already spent. Signed first: the wallet will
    // not sign for an output it knows is spent.
    let double_spend =
        signed_spend_hex(h.rpc(), outpoint, value, 9_000).await?;
    let spent = spend_output(h.rpc(), outpoint, value, 5_000).await?;
    let block =
        generate_block(h.rpc(), &h.node.mining_address, &[spent]).await?;
    h.wait_for_tip(block).await?;
    ensure_rejected(&h.submit(&double_spend).await?, "missingorspent")?;

    anyhow::ensure!(
        h.server.list_slipstream_txs().await?.is_empty(),
        "a refused tx is in the pool"
    );
    Ok(())
}

/// A slipstream tx that double spends a node tx competes with it by fee rate,
/// and the loser is gone once the winner is mined.
async fn conflicts_resolved_by_fee(h: &Harness) -> anyhow::Result<()> {
    // Slipstream pays more: it replaces the node's tx in the template.
    let (outpoint, value) = h.fund().await?;
    let node_tx = spend_output(h.rpc(), outpoint, value, 5_000).await?;
    h.wait_until("node tx in template", || async {
        Ok(h.template_txids().await?.contains(&node_tx))
    })
    .await?;
    let winner = h
        .submit_accepted(
            &signed_spend_hex(h.rpc(), outpoint, value, 50_000).await?,
        )
        .await?;
    anyhow::ensure!(
        winner.conflicts_with.contains(&node_tx),
        "conflict with the node's tx not declared: {winner:?}"
    );
    let template = h.template_txids().await?;
    anyhow::ensure!(
        template.contains(&winner.txid) && !template.contains(&node_tx),
        "expected the slipstream tx in place of the node's: {template:?}"
    );
    let block = h.mine_template().await?;
    h.wait_for_status(winner.txid, "winner mined", |status| {
        matches!(status, SlipstreamTxStatus::Removed {
            removal: SlipstreamRemoval::Mined { block_hash }, ..
        } if *block_hash == block)
    })
    .await?;

    // The node's tx pays more: the slipstream tx loses, and is evicted when
    // the block spending its input connects.
    let (outpoint, value) = h.fund().await?;
    let loser = h
        .submit_accepted(
            &signed_spend_hex(h.rpc(), outpoint, value, 2_000).await?,
        )
        .await?;
    let node_tx = spend_output(h.rpc(), outpoint, value, 60_000).await?;
    h.wait_until("node tx in template", || async {
        Ok(h.template_txids().await?.contains(&node_tx))
    })
    .await?;
    anyhow::ensure!(
        !h.template_txids().await?.contains(&loser.txid),
        "both sides of a double spend in the template"
    );
    let block = h.mine_template().await?;
    h.wait_for_status(loser.txid, "loser evicted", |status| {
        *status
            == SlipstreamTxStatus::Removed {
                txid: loser.txid,
                removal: SlipstreamRemoval::ConflictMined {
                    block_hash: block,
                    spent_by: node_tx,
                },
            }
    })
    .await?;
    Ok(())
}

/// A slipstream tx may spend another, and withdrawing the parent takes the
/// child with it.
async fn package_and_withdraw(h: &Harness) -> anyhow::Result<()> {
    let (outpoint, value) = h.fund().await?;
    let parent_hex = unrelayable_spend_hex(h.rpc(), outpoint, value).await?;
    let parent = h.submit_accepted(&parent_hex).await?;
    let child_hex =
        child_of_unrelayed_hex(h.rpc(), &decode(&parent_hex)?, 3_000).await?;
    let child = h.submit_accepted(&child_hex).await?;

    let template = h.template_txids().await?;
    let position = |txid| template.iter().position(|t| *t == txid);
    anyhow::ensure!(
        matches!((position(parent.txid), position(child.txid)), (Some(p), Some(c)) if p < c),
        "expected parent before child in the template: {template:?}"
    );

    let mut removed = h.server.remove_slipstream_tx(parent.txid).await?;
    removed.sort();
    let mut expected = vec![parent.txid, child.txid];
    expected.sort();
    anyhow::ensure!(removed == expected, "withdrew {removed:?}");
    let template = h.template_txids().await?;
    anyhow::ensure!(
        !template.contains(&parent.txid) && !template.contains(&child.txid),
        "withdrawn txs still in the template"
    );
    for txid in [parent.txid, child.txid] {
        anyhow::ensure!(
            h.status(txid).await?
                == SlipstreamTxStatus::Removed {
                    txid,
                    removal: SlipstreamRemoval::Withdrawn
                },
            "{txid} not withdrawn"
        );
    }
    Ok(())
}

/// A slipstream tx spending a node tx goes when the node drops that parent,
/// which the node itself never announces for the child.
async fn evicted_with_node_parent(h: &Harness) -> anyhow::Result<()> {
    let node_parent = submit_tx(h.rpc(), 1_000_000).await?;
    h.wait_until("node parent in template", || async {
        Ok(h.template_txids().await?.contains(&node_parent))
    })
    .await?;
    let (outpoint, value) = wallet_outputs_of(h.rpc(), node_parent)
        .await?
        .into_iter()
        .find(|(_, value)| *value == 1_000_000)
        .ok_or_else(|| anyhow!("{node_parent} has no 1_000_000 sat output"))?;
    let child = h
        .submit_accepted(
            &unrelayable_spend_hex(h.rpc(), outpoint, value).await?,
        )
        .await?;
    anyhow::ensure!(
        h.template_txids().await?.contains(&child.txid),
        "child missing from the template"
    );

    let _replacement: Txid = bump_fee(h.rpc(), node_parent).await?;
    h.wait_for_status(child.txid, "child evicted", |status| {
        *status
            == SlipstreamTxStatus::Removed {
                txid: child.txid,
                removal: SlipstreamRemoval::ParentRemoved {
                    parent: node_parent,
                },
            }
    })
    .await?;
    anyhow::ensure!(
        !h.template_txids().await?.contains(&child.txid),
        "orphaned child still in the template"
    );
    Ok(())
}

/// A spend of `outpoint` whose outputs carry `checkmultisigs` bare
/// `OP_CHECKMULTISIG`s, which count 20 legacy sigops, or 80 sigop cost, each.
/// Non-standard, and consensus-valid.
async fn sigop_heavy_spend_hex(
    rpc: &RpcClient,
    outpoint: OutPoint,
    value_sat: u64,
    checkmultisigs: usize,
) -> anyhow::Result<String> {
    let pay_to = get_new_address(rpc).await?;
    let checkmultisig = ScriptBuf::from_bytes(vec![
        bitcoin::opcodes::all::OP_CHECKMULTISIG.to_u8(),
    ]);
    let tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: outpoint,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: std::iter::once(TxOut {
            value: Amount::from_sat(value_sat - 20_000),
            script_pubkey: pay_to.script_pubkey(),
        })
        .chain(std::iter::repeat_n(
            TxOut {
                value: Amount::ZERO,
                script_pubkey: checkmultisig,
            },
            checkmultisigs,
        ))
        .collect(),
    };
    sign(rpc, &tx, serde_json::json!([])).await
}

/// Sigops are bounded per tx, and across the pool: template selection does
/// not count them, so nothing else stops slipstream txs from taking a block
/// past its limit.
async fn sigops_bounded(h: &Harness) -> anyhow::Result<()> {
    let (outpoint_a, value_a) = h.fund().await?;
    let (outpoint_b, value_b) = h.fund().await?;

    // 201 * 80 = 16_080, over the per-tx 16_000 before the input's own
    let too_heavy =
        sigop_heavy_spend_hex(h.rpc(), outpoint_a, value_a, 201).await?;
    ensure_rejected(&h.submit(&too_heavy).await?, "too-many-sigops")?;

    // 101 * 80 = 8_080 each, plus 1 for the P2WPKH input's signature check:
    // one fits the pool's 16_000, two do not
    let first =
        sigop_heavy_spend_hex(h.rpc(), outpoint_a, value_a, 101).await?;
    let first = h.submit_accepted(&first).await?;
    anyhow::ensure!(
        first.sigop_cost == Some(8_081),
        "unexpected sigop cost: {first:?}"
    );
    let second =
        sigop_heavy_spend_hex(h.rpc(), outpoint_b, value_b, 101).await?;
    ensure_rejected(&h.submit(&second).await?, "slipstream-sigops-full")?;

    // Withdrawing the first frees its budget
    let _removed: Vec<Txid> = h.server.remove_slipstream_tx(first.txid).await?;
    let _second = h.submit_accepted(&second).await?;
    let _mined = h.mine_template().await?;
    Ok(())
}

/// A disconnected block evicts every slipstream tx: the reorg may have left
/// any of them invalid, and the node never re-checks them.
async fn evicted_on_reorg(h: &Harness) -> anyhow::Result<()> {
    let (outpoint, value) = h.fund().await?;
    let tx = h
        .submit_accepted(
            &unrelayable_spend_hex(h.rpc(), outpoint, value).await?,
        )
        .await?;
    let tip = h.rpc().getbestblockhash().await?;
    h.rpc().invalidate_block(tip).await?;
    h.wait_for_status(tx.txid, "reorged", |status| {
        *status
            == SlipstreamTxStatus::Removed {
                txid: tx.txid,
                removal: SlipstreamRemoval::Reorged { block_hash: tip },
            }
    })
    .await?;
    anyhow::ensure!(
        !h.template_txids().await?.contains(&tx.txid),
        "reorged slipstream tx still in the template"
    );
    Ok(())
}

pub async fn test_slipstream(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let h = Harness::new(&bin_paths, directories).await?;
    mined_without_relay(&h)
        .await
        .context("mined without relay")?;
    invalid_refused(&h).await.context("invalid refused")?;
    conflicts_resolved_by_fee(&h)
        .await
        .context("conflicts resolved by fee")?;
    package_and_withdraw(&h)
        .await
        .context("package and withdraw")?;
    evicted_with_node_parent(&h)
        .await
        .context("evicted with node parent")?;
    sigops_bounded(&h).await.context("sigops bounded")?;
    // Last: it leaves the chain reorged
    evicted_on_reorg(&h).await.context("evicted on reorg")?;
    h.task_errors.ensure_empty("slipstream")?;
    Ok(())
}
