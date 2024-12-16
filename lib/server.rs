use std::convert::Infallible;

use async_trait::async_trait;
use bip300301::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, NetworkInfo,
};
use bitcoin::{
    amount::CheckedSum, hashes::Hash as _, Network, ScriptBuf, Transaction,
    TxOut, Txid,
};
use chrono::Utc;
use educe::Educe;
use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorCode};
use thiserror::Error;

use crate::{
    cusf_block_producer::{self, CusfBlockProducer, InitialBlockTemplate},
    mempool::MempoolSync,
};

#[rpc(server)]
pub trait Rpc {
    #[method(name = "getblocktemplate")]
    async fn get_block_template(
        &self,
        _request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate>;
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("Sample block template cannot set coinbasetxn field")]
    SampleBlockTemplate,
}

pub struct Server<Enforcer> {
    coinbase_spk: ScriptBuf,
    mempool: MempoolSync<Enforcer>,
    network: Network,
    network_info: NetworkInfo,
    sample_block_template: BlockTemplate,
}

impl<Enforcer> Server<Enforcer> {
    pub fn new(
        coinbase_spk: ScriptBuf,
        mempool: MempoolSync<Enforcer>,
        network: Network,
        network_info: NetworkInfo,
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
fn get_block_reward(
    height: u32,
    fees: bitcoin::Amount,
    network: Network,
) -> bitcoin::Amount {
    let subsidy_sats = 50 * bitcoin::Amount::ONE_BTC.to_sat();
    let subsidy_halving_interval = match network {
        Network::Regtest => 150,
        _ => bitcoin::constants::SUBSIDY_HALVING_INTERVAL,
    };
    let halvings = height / subsidy_halving_interval;
    if halvings >= 64 {
        fees
    } else {
        fees + bitcoin::Amount::from_sat(subsidy_sats >> halvings)
    }
}

const WITNESS_RESERVED_VALUE: [u8; 32] = [0; 32];

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

fn finalize_coinbase_tx(
    coinbase_spk: ScriptBuf,
    best_block_height: u32,
    network: Network,
    mut coinbase_txouts: Vec<TxOut>,
    transactions: &[BlockTemplateTransaction],
) -> Result<Transaction, FinalizeCoinbaseTxError> {
    let bip34_height_script = bitcoin::blockdata::script::Builder::new()
        .push_int((best_block_height + 1) as i64)
        .push_opcode(bitcoin::opcodes::OP_0)
        .into_script();
    let fees = transactions.iter().enumerate().try_fold(
        bitcoin::Amount::ZERO,
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
    let block_reward = get_block_reward(best_block_height + 1, fees, network);
    // Remaining block reward value to add to coinbase txouts
    let coinbase_reward = coinbase_txouts.iter().try_fold(
        block_reward,
        |reward_acc, txout| {
            reward_acc
                .checked_sub(txout.value)
                .ok_or(FinalizeCoinbaseTxError::CoinbaseRewardUnderflow)
        },
    )?;
    if coinbase_reward > bitcoin::Amount::ZERO {
        coinbase_txouts.push(TxOut {
            value: coinbase_reward,
            script_pubkey: coinbase_spk,
        })
    }
    Ok(Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![bitcoin::TxIn {
            previous_output: bitcoin::OutPoint {
                txid: Txid::all_zeros(),
                vout: 0xFFFF_FFFF,
            },
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::from_slice(&[WITNESS_RESERVED_VALUE]),
            script_sig: bip34_height_script,
        }],
        output: coinbase_txouts,
    })
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
enum BuildBlockError<BP>
where
    BP: CusfBlockProducer,
{
    #[error(transparent)]
    InitialBlockTemplate(BP::InitialBlockTemplateError),
    #[error(transparent)]
    FinalizeCoinbaseTx(#[from] FinalizeCoinbaseTxError),
    #[error(transparent)]
    MempoolInsert(#[from] crate::mempool::MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] crate::mempool::MempoolRemoveError),
    #[error(transparent)]
    SuffixTxs(BP::SuffixTxsError),
}

// select block txs, and coinbase txouts if coinbasetxn is set
fn block_txs<const COINBASE_TXN: bool, BP>(block_producer: &BP, mempool: &crate::mempool::Mempool)
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
    initial_block_template = block_producer
        .initial_block_template(
            typewit::MakeTypeWitness::MAKE,
            initial_block_template,
        )
        .map_err(BuildBlockError::InitialBlockTemplate)?;
    let mut mempool = mempool.clone();
    for (tx, fee) in initial_block_template.prefix_txs.iter().cloned() {
        let _txinfo: Option<_> = mempool.insert(tx, fee.to_sat())?;
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
    {
        // Remove prefix txs
        let prefix_txids: std::collections::HashSet<Txid> =
            initial_block_template
                .prefix_txs
                .iter()
                .map(|(tx, _fee)| tx.compute_txid())
                .collect();
        let _removed_txs = mempool
            .try_filter(false, |tx, _| {
                let txid = tx.compute_txid();
                Result::<_, Infallible>::Ok(!prefix_txids.contains(&txid))
            })
            .map_err(|err| match err {
                either::Either::Left(err) => err,
            })?;
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
    let mempool_txs = mempool.propose_txs()?;
    initial_block_template.prefix_txs.extend(
        mempool_txs
            .iter()
            .map(|tx| bitcoin::consensus::deserialize(&tx.data).unwrap()),
    );
    let suffix_txs = block_producer
        .suffix_txs(typewit::MakeTypeWitness::MAKE, &initial_block_template)
        .map_err(BuildBlockError::SuffixTxs)?;
    res_txs.extend(mempool_txs);
    res_txs.extend(suffix_txs.iter().map(|(tx, fee)| {
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
    Ok((initial_block_template.coinbase_txouts, res_txs))
}

#[async_trait]
impl<BP> RpcServer for Server<BP>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
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
            // FIXME: compute this
            ref coinbase_txn_or_value,
            ref mutable,
            sigop_limit,
            size_limit,
            weight_limit,
            ref signet_challenge,
            ..
        } = self.sample_block_template;

        let (
            target,
            prev_blockhash,
            tip_block_mediantime,
            tip_block_height,
            coinbase_txn,
            block_txs,
        ) = self
            .mempool
            .with(|mempool, enforcer| {
                let tip_block = mempool.tip();
                let (coinbase_txn, block_txs) =
                    if request.capabilities.contains("coinbasetxn") {
                        let (coinbase_txouts, block_txs) =
                            block_txs::<true, _>(enforcer, mempool)?;
                        let coinbase_tx = finalize_coinbase_tx(
                            self.coinbase_spk.clone(),
                            tip_block.height,
                            self.network,
                            coinbase_txouts,
                            &block_txs,
                        )?;
                        (Some(coinbase_tx), block_txs)
                    } else {
                        let ((), block_txs) =
                            block_txs::<false, _>(enforcer, mempool)?;
                        (None, block_txs)
                    };
                Ok((
                    mempool.next_target(),
                    tip_block.hash,
                    tip_block.mediantime,
                    tip_block.height,
                    coinbase_txn,
                    block_txs,
                ))
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
        let current_time_adjusted =
            (now.timestamp() + self.network_info.time_offset_s) as u64;
        let mintime = std::cmp::max(
            tip_block_mediantime as u64 + 1,
            current_time_adjusted,
        );
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
            // FIXME: set this
            default_witness_commitment: None,
            signet_challenge: signet_challenge.clone(),
        };
        Ok(res)
    }
}
