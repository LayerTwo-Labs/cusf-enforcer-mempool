use std::convert::Infallible;

use async_trait::async_trait;
use bip300301::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, NetworkInfo,
};
use bitcoin::{
    amount::CheckedSum, hashes::Hash as _, merkle_tree, script::PushBytesBuf,
    Amount, Block, Network, Script, ScriptBuf, Transaction, TxOut, Txid,
    WitnessMerkleNode, Wtxid,
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

/// Signer for signet blocks
pub trait SignetSigner {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Sign for a specific signet challenge, returning a scriptSig and
    /// scriptWitness
    fn sign(
        &self,
        signet_challenge: &Script,
    ) -> Result<(ScriptBuf, bitcoin::Witness), Self::Error>;
}

/// [`SignetSigner`] to use when Signet signing is not implemented
#[derive(Debug, Default, Error)]
#[error("Signet signing is not implemented")]
pub struct SignetSigningNotImplemented;

impl SignetSigner for SignetSigningNotImplemented {
    type Error = SignetSigningNotImplemented;

    fn sign(
        &self,
        _signet_challenge: &Script,
    ) -> Result<(ScriptBuf, bitcoin::Witness), Self::Error> {
        Err(SignetSigningNotImplemented)
    }
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("Sample block template cannot set coinbasetxn field")]
    SampleBlockTemplate,
}

pub struct Server<Enforcer, SignetSigner = SignetSigningNotImplemented> {
    coinbase_spk: ScriptBuf,
    mempool: MempoolSync<Enforcer>,
    network: Network,
    network_info: NetworkInfo,
    sample_block_template: BlockTemplate,
    signet_signer: SignetSigner,
}

impl<Enforcer, SignetSigner> Server<Enforcer, SignetSigner> {
    pub fn new_with_signet_signer(
        coinbase_spk: ScriptBuf,
        mempool: MempoolSync<Enforcer>,
        network: Network,
        network_info: NetworkInfo,
        sample_block_template: BlockTemplate,
        signet_signer: SignetSigner,
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
            signet_signer,
        })
    }
}

impl<Enforcer> Server<Enforcer> {
    pub fn new(
        coinbase_spk: ScriptBuf,
        mempool: MempoolSync<Enforcer>,
        network: Network,
        network_info: NetworkInfo,
        sample_block_template: BlockTemplate,
    ) -> Result<Self, CreateServerError> {
        Self::new_with_signet_signer(
            coinbase_spk,
            mempool,
            network,
            network_info,
            sample_block_template,
            SignetSigningNotImplemented,
        )
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

/// Compute witness commitment spk, from block txs not including coinbase tx.
/// Signet challenge should be `Some` for signets, and `None` otherwise.
fn witness_commitment_spk<Signer>(
    transactions: &[BlockTemplateTransaction],
    signet_challenge: Option<&Script>,
    signet_signer: &Signer,
) -> Result<ScriptBuf, Signer::Error>
where
    Signer: SignetSigner,
{
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
    let mut res = {
        const WITNESS_COMMITMENT_HEADER: [u8; 4] = [0xaa, 0x21, 0xa9, 0xed];
        let mut push_bytes = PushBytesBuf::from(WITNESS_COMMITMENT_HEADER);
        let () = push_bytes
            .extend_from_slice(witness_commitment.as_byte_array())
            .unwrap();
        ScriptBuf::new_op_return(push_bytes)
    };
    if let Some(signet_challenge) = signet_challenge {
        const SIGNET_HEADER: [u8; 4] = [0xec, 0xc7, 0xda, 0xa2];
        let mut push_bytes = PushBytesBuf::from(SIGNET_HEADER);
        let (script_sig, script_witness) =
            signet_signer.sign(signet_challenge)?;
        push_bytes.extend_from_slice(script_sig.as_bytes()).unwrap();
        // FIXME: is this correct?
        let script_witness_bytes =
            bitcoin::consensus::serialize(&script_witness);
        push_bytes.extend_from_slice(&script_witness_bytes).unwrap();
        res.push_slice(push_bytes);
    }
    Ok(res)
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
enum FinalizeCoinbaseTxError<Signer>
where
    Signer: SignetSigner,
{
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
    #[error("Failed to sign signet block")]
    Signet(#[source] <Signer as SignetSigner>::Error),
}

// Finalize coinbase tx, returning the coinbase tx and witness commitment spk
fn finalize_coinbase_tx<Signer>(
    coinbase_spk: ScriptBuf,
    best_block_height: u32,
    network: Network,
    mut coinbase_txouts: Vec<TxOut>,
    transactions: &[BlockTemplateTransaction],
    signet_challenge: Option<&Script>,
    signet_signer: &Signer,
) -> Result<(Transaction, ScriptBuf), FinalizeCoinbaseTxError<Signer>>
where
    Signer: SignetSigner,
{
    let bip34_height_script = bitcoin::blockdata::script::Builder::new()
        .push_int((best_block_height + 1) as i64)
        .push_opcode(bitcoin::opcodes::OP_0)
        .into_script();
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
    if coinbase_reward > Amount::ZERO {
        coinbase_txouts.push(TxOut {
            value: coinbase_reward,
            script_pubkey: coinbase_spk,
        })
    }
    let witness_commitment_spk =
        witness_commitment_spk(transactions, signet_challenge, signet_signer)
            .map_err(FinalizeCoinbaseTxError::Signet)?;
    // Add witness commitment to coinbase txouts if not present
    if !coinbase_txouts
        .iter()
        .any(|txout| txout.script_pubkey == witness_commitment_spk)
    {
        coinbase_txouts.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: witness_commitment_spk.clone(),
        })
    }
    let res = Transaction {
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
    };
    Ok((res, witness_commitment_spk))
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
enum BuildBlockError<BP, Signer>
where
    BP: CusfBlockProducer,
    Signer: SignetSigner,
{
    #[error(transparent)]
    FinalizeCoinbaseTx(#[from] FinalizeCoinbaseTxError<Signer>),
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
fn block_txs<const COINBASE_TXN: bool, BP, Signer>(block_producer: &BP, mempool: &crate::mempool::Mempool)
    -> Result<
            (<typewit::const_marker::Bool<COINBASE_TXN> as cusf_block_producer::CoinbaseTxn>::CoinbaseTxouts,
             Vec<BlockTemplateTransaction>),
            BuildBlockError<BP, Signer>
        >
    where BP: CusfBlockProducer,
        Signer: SignetSigner,
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
impl<BP, Signer> RpcServer for Server<BP, Signer>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
    Signer: SignetSigner + Send + Sync + 'static,
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
                let tip_block = mempool.tip();
                let (coinbase_txn, block_txs, default_witness_commitment) =
                    if request.capabilities.contains("coinbasetxn") {
                        let (coinbase_txouts, block_txs) =
                            block_txs::<true, _, _>(enforcer, mempool)?;
                        let (coinbase_tx, witness_commitment_spk) =
                            finalize_coinbase_tx(
                                self.coinbase_spk.clone(),
                                tip_block.height,
                                self.network,
                                coinbase_txouts,
                                &block_txs,
                                signet_challenge
                                    .as_ref()
                                    .map(ScriptBuf::as_script),
                                &self.signet_signer,
                            )?;
                        let default_witness_commitment =
                            witness_commitment_spk.to_bytes();
                        (
                            Some(coinbase_tx),
                            block_txs,
                            Some(default_witness_commitment),
                        )
                    } else {
                        let ((), block_txs) =
                            block_txs::<false, _, _>(enforcer, mempool)?;
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
            })
            .await
            .ok_or_else(|| {
                let err = anyhow::anyhow!("Mempool unavailable");
                let err = log_error(err);
                internal_error(err)
            })?
            .map_err(|err: BuildBlockError<_, _>| {
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
            default_witness_commitment,
            signet_challenge: signet_challenge.clone(),
        };
        Ok(res)
    }
}
