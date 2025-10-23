use std::convert::Infallible;

use async_trait::async_trait;
use bitcoin::{
    amount::CheckedSum, hashes::Hash as _, merkle_tree, script::PushBytesBuf,
    Amount, Block, BlockHash, Network, ScriptBuf, Transaction, TxOut, Txid,
    WitnessMerkleNode, Wtxid,
};
use bitcoin_jsonrpsee::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, NetworkInfo,
};
use chrono::Utc;
use educe::Educe;
use futures::FutureExt;
use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorCode};
use thiserror::Error;

use crate::{
    cusf_block_producer::{self, CusfBlockProducer, InitialBlockTemplate},
    mempool::MempoolSync,
};

#[rpc(client, server)]
pub trait Rpc {
    #[method(name = "getblocktemplate")]
    async fn get_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate>;

    /// Returns None if the block is invalid, otherwise the error code
    /// describing why the block was rejected.
    #[method(name = "submitblock")]
    async fn submit_block(
        &self,
        block_hex: String,
    ) -> RpcResult<Option<String>>;
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("Sample block template cannot set coinbasetxn field")]
    SampleBlockTemplate,
}

pub struct Server<Enforcer, RpcClient> {
    coinbase_spk: ScriptBuf,
    mempool: MempoolSync<Enforcer>,
    network: Network,
    network_info: NetworkInfo,
    rpc_client: RpcClient,
    sample_block_template: BlockTemplate,
}

impl<Enforcer, RpcClient> Server<Enforcer, RpcClient> {
    pub fn new(
        coinbase_spk: ScriptBuf,
        mempool: MempoolSync<Enforcer>,
        network: Network,
        network_info: NetworkInfo,
        rpc_client: RpcClient,
        sample_block_template: BlockTemplate,
    ) -> Result<Self, CreateServerError> {
        if matches!(
            sample_block_template.coinbase_txn_or_value,
            CoinbaseTxnOrValue::Txn(_)
        ) {
            return Err(CreateServerError::SampleBlockTemplate);
        };
        Ok(Self {
            coinbase_spk,
            mempool,
            network,
            network_info,
            rpc_client,
            sample_block_template,
        })
    }
}

fn log_error<Err>(err: Err) -> anyhow::Error
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    tracing::error!("{err:#}");
    err
}

fn internal_error<Err>(err: Err) -> jsonrpsee::types::ErrorObjectOwned
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    let err_msg = format!("{err:#}");
    jsonrpsee::types::ErrorObjectOwned::owned(
        ErrorCode::InternalError.code(),
        ErrorCode::InternalError.message(),
        Some(err_msg),
    )
}

/// Compute the block reward for the specified height
fn get_block_reward(height: u32, fees: Amount, network: Network) -> Amount {
    let subsidy_sats = 50 * Amount::ONE_BTC.to_sat();
    let subsidy_halving_interval = match network {
        Network::Regtest => 150,
        _ => bitcoin::constants::SUBSIDY_HALVING_INTERVAL,
    };
    let halvings = height / subsidy_halving_interval;
    if halvings >= 64 {
        fees
    } else {
        fees + Amount::from_sat(subsidy_sats >> halvings)
    }
}

const WITNESS_RESERVED_VALUE: [u8; 32] = [0; 32];

/// Add witness commitment output to the coinbase tx, and return a copy of the
/// witness commitment spk.
/// The coinbase tx should not include the witness commitment txout.
/// Signet challenge should be `Some` for signets, and `None` otherwise.
fn add_witness_commitment_output(
    coinbase_tx: &mut Transaction,
    transactions: &[BlockTemplateTransaction],
) -> ScriptBuf {
    let witness_root = {
        let hashes = std::iter::once(Wtxid::all_zeros().to_raw_hash())
            .chain(transactions.iter().map(|tx| tx.hash.to_raw_hash()));
        merkle_tree::calculate_root(hashes)
            .map(WitnessMerkleNode::from_raw_hash)
            .unwrap()
    };
    let witness_commitment = Block::compute_witness_commitment(
        &witness_root,
        &WITNESS_RESERVED_VALUE,
    );
    // https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki#commitment-structure
    let witness_commitment_spk = {
        const WITNESS_COMMITMENT_HEADER: [u8; 4] = [0xaa, 0x21, 0xa9, 0xed];
        let mut push_bytes = PushBytesBuf::from(WITNESS_COMMITMENT_HEADER);
        let () = push_bytes
            .extend_from_slice(witness_commitment.as_byte_array())
            .unwrap();
        ScriptBuf::new_op_return(push_bytes)
    };
    coinbase_tx.output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: witness_commitment_spk.clone(),
    });
    witness_commitment_spk
}

#[derive(Debug, Error)]
enum FinalizeCoinbaseTxError {
    #[error("Coinbase reward underflow")]
    CoinbaseRewardUnderflow,
    #[error("Fee overflow")]
    FeeOverflow,
    #[error(
        "Negative tx fee for tx `{txid}` at index `{tx_index}`: `{}`",
        .fee.display_dynamic()
    )]
    NegativeTxFee {
        txid: Txid,
        tx_index: usize,
        fee: bitcoin::SignedAmount,
    },
}

/// Generate a BIP34 height script
fn bip34_height_script(height: u32) -> ScriptBuf {
    let mut builder =
        bitcoin::blockdata::script::Builder::new().push_int(height as i64);
    while builder.len() < 2 {
        builder = builder.push_opcode(bitcoin::opcodes::OP_0);
    }
    builder.into_script()
}

/// Finalize coinbase tx.
/// The witness commitment output in the coinbase is not added on signets.
/// Returns the coinbase tx, and (on networks other than signets) the witness
/// commitment spk.
fn finalize_coinbase_tx(
    coinbase_spk: ScriptBuf,
    block_height: u32,
    network: Network,
    mut coinbase_txouts: Vec<TxOut>,
    transactions: &[BlockTemplateTransaction],
) -> Result<(Transaction, Option<ScriptBuf>), FinalizeCoinbaseTxError> {
    let fees = transactions.iter().enumerate().try_fold(
        Amount::ZERO,
        |fees_acc, (tx_index, tx)| {
            let fee = tx.fee.to_unsigned().map_err(|_| {
                FinalizeCoinbaseTxError::NegativeTxFee {
                    txid: tx.txid,
                    tx_index,
                    fee: tx.fee,
                }
            })?;
            fees_acc
                .checked_add(fee)
                .ok_or(FinalizeCoinbaseTxError::FeeOverflow)
        },
    )?;
    let block_reward = get_block_reward(block_height, fees, network);
    // Remaining block reward value to add to coinbase txouts
    let coinbase_reward = coinbase_txouts.iter().try_fold(
        block_reward,
        |reward_acc, txout| {
            reward_acc
                .checked_sub(txout.value)
                .ok_or(FinalizeCoinbaseTxError::CoinbaseRewardUnderflow)
        },
    )?;
    tracing::debug!(
        block_reward = %block_reward.display_dynamic(),
        coinbase_reward = %coinbase_reward.display_dynamic(),
        fees = %fees.display_dynamic(),
    );
    if coinbase_reward > Amount::ZERO {
        coinbase_txouts.push(TxOut {
            value: coinbase_reward,
            script_pubkey: coinbase_spk,
        })
    }
    let mut coinbase_tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![bitcoin::TxIn {
            previous_output: bitcoin::OutPoint {
                txid: Txid::all_zeros(),
                vout: 0xFFFF_FFFF,
            },
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::from_slice(&[WITNESS_RESERVED_VALUE]),
            script_sig: bip34_height_script(block_height),
        }],
        output: coinbase_txouts,
    };
    let witness_commitment_spk = match network {
        Network::Signet => None,
        _ => Some(add_witness_commitment_output(
            &mut coinbase_tx,
            transactions,
        )),
    };
    Ok((coinbase_tx, witness_commitment_spk))
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
enum BuildBlockError<BP>
where
    BP: CusfBlockProducer,
{
    #[error(transparent)]
    FinalizeCoinbaseTx(#[from] FinalizeCoinbaseTxError),
    #[error(transparent)]
    InitialBlockTemplate(BP::InitialBlockTemplateError),
    #[error(transparent)]
    MempoolInsert(#[from] crate::mempool::MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] crate::mempool::MempoolRemoveError),
    #[error(transparent)]
    SuffixTxs(BP::SuffixTxsError),
}

// select block txs, and coinbase txouts if coinbasetxn is set
async fn block_txs<const COINBASE_TXN: bool, BP>(
    block_producer: &BP,
    mempool: &crate::mempool::Mempool,
    parent_block_hash: &BlockHash,
)
    -> Result<
            (<typewit::const_marker::Bool<COINBASE_TXN> as cusf_block_producer::CoinbaseTxn>::CoinbaseTxouts,
             Vec<BlockTemplateTransaction>),
            BuildBlockError<BP>
        >
    where BP: CusfBlockProducer,
    typewit::const_marker::Bool<COINBASE_TXN>: cusf_block_producer::CoinbaseTxn
     {
    let mut initial_block_template =
        InitialBlockTemplate::<COINBASE_TXN>::default();
    tracing::debug!("Generating initial block template");
    initial_block_template = block_producer
        .initial_block_template(
            parent_block_hash,
            typewit::MakeTypeWitness::MAKE,
            initial_block_template,
        )
        .await
        .map_err(BuildBlockError::InitialBlockTemplate)?;
    let prefix_txids: hashlink::LinkedHashSet<Txid> = initial_block_template
        .prefix_txs
        .iter()
        .map(|(tx, _fee)| tx.compute_txid())
        .collect();
    {
        let prefix_txids: String = format!(
            "[{}]",
            prefix_txids
                .iter()
                .map(|txid| txid.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        tracing::debug!(%prefix_txids);
    }
    let mut mempool = mempool.clone();
    tracing::debug!("Inserting prefix txs into cloned mempool");
    for (tx, fee) in initial_block_template.prefix_txs.iter().cloned() {
        match mempool.insert(tx, fee.to_sat(), Default::default()) {
            Ok(_)
            | Err(crate::mempool::MempoolInsertError::TxAlreadyExists {
                ..
            }) => (),
            Err(err) => return Err(err.into()),
        }
    }
    // depends field must be set later
    let mut res_txs: Vec<_> = {
        initial_block_template
            .prefix_txs
            .iter()
            .map(|(tx, fee)| BlockTemplateTransaction {
                data: bitcoin::consensus::serialize(tx),
                txid: tx.compute_txid(),
                hash: tx.compute_wtxid(),
                depends: Vec::new(),
                fee: (*fee).try_into().unwrap(),
                sigops: None,
                weight: tx.weight().to_wu(),
            })
            .collect()
    };
    // Remove prefix txs
    {
        tracing::debug!("Removing prefix txs");
        let _removed_txs = mempool
            .try_filter(false, |tx, _| {
                let txid = tx.compute_txid();
                Result::<_, Infallible>::Ok(!prefix_txids.contains(&txid))
            })
            .map_err(|err| match err {
                either::Either::Left(err) => err,
            })?;
        {
            let excluded_txids: String = format!(
                "[{}]",
                initial_block_template
                    .exclude_mempool_txs
                    .iter()
                    .map(|txid| txid.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            tracing::debug!(%excluded_txids, "Removing excluded txs");
        }
        let _removed_txs = mempool
            .try_filter(true, |tx, _| {
                let txid = tx.compute_txid();
                Result::<_, Infallible>::Ok(
                    !initial_block_template.exclude_mempool_txs.contains(&txid),
                )
            })
            .map_err(|err| match err {
                either::Either::Left(err) => err,
            })?;
    }
    tracing::debug!("Proposing txs for inclusion in block");
    let mempool_txs = mempool.propose_txs()?;
    {
        let proposed_txids: String = format!(
            "[{}]",
            mempool_txs
                .iter()
                .map(|tx| tx.txid.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        tracing::debug!(%proposed_txids, "Proposed txs for inclusion in block");
    }
    initial_block_template
        .prefix_txs
        .extend(mempool_txs.iter().map(|tx| {
            let fee = tx.fee.unsigned_abs();
            let tx = bitcoin::consensus::deserialize(&tx.data).unwrap();
            (tx, fee)
        }));
    tracing::debug!("Adding block template suffix");
    let template_suffix = block_producer
        .block_template_suffix(
            parent_block_hash,
            typewit::MakeTypeWitness::MAKE,
            &initial_block_template,
        )
        .await
        .map_err(BuildBlockError::SuffixTxs)?;
    let mut res_coinbase_txouts = initial_block_template.coinbase_txouts;
    match typewit::MakeTypeWitness::MAKE {
        typewit::const_marker::BoolWit::True(wit) => {
            let wit = wit.map(cusf_block_producer::CoinbaseTxouts);
            wit.in_mut()
                .to_right(&mut res_coinbase_txouts)
                .extend(wit.to_right(template_suffix.coinbase_txouts));
        }
        typewit::const_marker::BoolWit::False(_) => (),
    }
    res_txs.extend(mempool_txs);
    res_txs.extend(template_suffix.txs.iter().map(|(tx, fee)| {
        BlockTemplateTransaction {
            data: bitcoin::consensus::serialize(tx),
            txid: tx.compute_txid(),
            hash: tx.compute_wtxid(),
            depends: Vec::new(),
            fee: (*fee).try_into().unwrap(),
            sigops: None,
            weight: tx.weight().to_wu(),
        }
    }));
    // Fill depends
    {
        let mut tx_indexes = std::collections::HashMap::new();
        for (idx, tx) in res_txs.iter_mut().enumerate() {
            tx_indexes.insert(tx.txid, idx as u32);
            let tx_inputs =
                bitcoin::consensus::deserialize::<Transaction>(&tx.data)
                    .unwrap()
                    .input;
            for txin in tx_inputs {
                if let Some(tx_idx) = tx_indexes.get(&txin.previous_output.txid)
                {
                    tx.depends.push(*tx_idx)
                }
            }
            tx.depends.sort();
            tx.depends.dedup();
        }
    }
    Ok((res_coinbase_txouts, res_txs))
}

#[async_trait]
impl<BP, RpcClient> RpcServer for Server<BP, RpcClient>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Send + Sync + 'static,
{
    async fn get_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate> {
        const NONCE_RANGE: [u8; 8] = [0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

        let now = Utc::now();
        let BlockTemplate {
            version,
            ref rules,
            ref version_bits_available,
            version_bits_required,
            ref coinbase_aux,
            ref coinbase_txn_or_value,
            ref mutable,
            sigop_limit,
            size_limit,
            weight_limit,
            ref signet_challenge,
            ..
        } = self.sample_block_template;
        let current_time_adjusted =
            (now.timestamp() + self.network_info.time_offset_s) as u64;
        let (
            target,
            prev_blockhash,
            tip_block_mediantime,
            tip_block_height,
            coinbase_txn,
            block_txs,
            default_witness_commitment,
        ) = self
            .mempool
            .with(|mempool, enforcer| {
                {
                    let coinbase_spk = self.coinbase_spk.clone();
                    let network = self.network;
                    async move {
                        let tip_block = mempool.tip();
                        let (
                            coinbase_txn,
                            block_txs,
                            default_witness_commitment,
                        ) = if request.capabilities.contains("coinbasetxn") {
                            tracing::debug!("Filling block txs");
                            let (coinbase_txouts, block_txs) =
                                block_txs::<true, _>(
                                    enforcer,
                                    mempool,
                                    &tip_block.hash,
                                )
                                .await?;
                            tracing::debug!("Finalizing coinbase txn");
                            let (coinbase_tx, witness_commitment_spk) =
                                finalize_coinbase_tx(
                                    coinbase_spk,
                                    tip_block.height + 1,
                                    network,
                                    coinbase_txouts,
                                    &block_txs,
                                )?;
                            let default_witness_commitment =
                                witness_commitment_spk
                                    .map(|spk| spk.to_bytes());
                            (
                                Some(coinbase_tx),
                                block_txs,
                                default_witness_commitment,
                            )
                        } else {
                            let ((), block_txs) = block_txs::<false, _>(
                                enforcer,
                                mempool,
                                &tip_block.hash,
                            )
                            .await?;
                            (None, block_txs, None)
                        };
                        Ok((
                            mempool.next_target(),
                            tip_block.hash,
                            tip_block.mediantime,
                            tip_block.height,
                            coinbase_txn,
                            block_txs,
                            default_witness_commitment,
                        ))
                    }
                }
                .boxed()
            })
            .await
            .ok_or_else(|| {
                let err = anyhow::anyhow!("Mempool unavailable");
                let err = log_error(err);
                internal_error(err)
            })?
            .map_err(|err: BuildBlockError<_>| {
                let err = log_error(err);
                internal_error(err)
            })?;
        let coinbase_txn_or_value = if let Some(coinbase_txn) = coinbase_txn {
            let fee = coinbase_txn
                .output
                .iter()
                .map(|txout| txout.value)
                .checked_sum()
                .ok_or_else(|| {
                    let err = anyhow::anyhow!(
                        "Value overflow error in coinbase output"
                    );
                    let err = log_error(err);
                    internal_error(err)
                })?;
            let txn = BlockTemplateTransaction {
                txid: coinbase_txn.compute_txid(),
                hash: coinbase_txn.compute_wtxid(),
                depends: Vec::new(),
                fee: -bitcoin::SignedAmount::try_from(fee).unwrap(),
                sigops: None,
                weight: coinbase_txn.weight().to_wu(),
                data: bitcoin::consensus::serialize(&coinbase_txn),
            };
            CoinbaseTxnOrValue::Txn(txn)
        } else {
            coinbase_txn_or_value.clone()
        };
        let mintime =
            // TODO: calculate this correctly
            /*
            std::cmp::max(
                tip_block_mediantime as u64 + 1,
                current_time_adjusted,
            )
            */
            tip_block_mediantime as u64 + 1;
        let height = tip_block_height + 1;
        let res = BlockTemplate {
            version,
            rules: rules.clone(),
            version_bits_available: version_bits_available.clone(),
            version_bits_required,
            prev_blockhash,
            transactions: block_txs,
            coinbase_aux: coinbase_aux.clone(),
            coinbase_txn_or_value,
            long_poll_id: None,
            target: target.to_le_bytes(),
            mintime,
            mutable: mutable.clone(),
            nonce_range: NONCE_RANGE,
            sigop_limit,
            size_limit,
            weight_limit,
            current_time: current_time_adjusted,
            compact_target: target.to_compact_lossy(),
            height,
            default_witness_commitment,
            signet_challenge: signet_challenge.clone(),
        };
        Ok(res)
    }

    async fn submit_block(
        &self,
        block_hex: String,
    ) -> RpcResult<Option<String>> {
        self.rpc_client
            .submit_block(block_hex)
            .await
            .map_err(|err| match err {
                jsonrpsee::core::ClientError::Call(err) => err,
                err => internal_error(err),
            })
    }
}
