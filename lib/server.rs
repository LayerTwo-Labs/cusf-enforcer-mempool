use std::convert::Infallible;

use async_trait::async_trait;
use bip300301::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, NetworkInfo,
};
use bitcoin::{
    amount::CheckedSum, hashes::Hash as _, merkle_tree, script::PushBytesBuf,
    Amount, Block, BlockHash, Network, OutPoint, Script, ScriptBuf,
    Transaction, TxIn, TxOut, Txid, WitnessMerkleNode, Wtxid,
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

/// [`bitcoin::psbt::GetKey`] impl to use if Signet signing is not implemented
#[derive(Debug, Default)]
pub struct SignetSigningNotImplemented;

impl bitcoin::psbt::GetKey for SignetSigningNotImplemented {
    type Error = Infallible;

    fn get_key<C: bitcoin::secp256k1::Signing>(
        &self,
        _key_request: bitcoin::psbt::KeyRequest,
        _secp: &bitcoin::key::Secp256k1<C>,
    ) -> Result<Option<bitcoin::PrivateKey>, Self::Error> {
        Ok(None)
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

/// Generate 'to_spend' tx for signing a signet block
fn signet_spend_tx(
    block_version: bitcoin::block::Version,
    prev_blockhash: BlockHash,
    signet_merkle_root: WitnessMerkleNode,
    block_timestamp: u32,
    challenge_script: ScriptBuf,
) -> Transaction {
    let mut block_data = PushBytesBuf::new();
    block_data
        .extend_from_slice(&block_version.to_consensus().to_le_bytes())
        .unwrap();
    block_data
        .extend_from_slice(prev_blockhash.as_byte_array())
        .unwrap();
    block_data
        .extend_from_slice(signet_merkle_root.as_byte_array())
        .unwrap();
    block_data
        .extend_from_slice(&block_timestamp.to_le_bytes())
        .unwrap();
    let script_sig = Script::builder()
        .push_opcode(bitcoin::opcodes::OP_0)
        .push_slice(&block_data)
        .into_script();
    Transaction {
        version: bitcoin::transaction::Version(0),
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::all_zeros(),
                vout: 0xFFFFFFFF,
            },
            script_sig,
            sequence: bitcoin::Sequence(0),
            witness: bitcoin::Witness::default(),
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: challenge_script,
        }],
    }
}

/// Generate 'to_sign' tx for signing a signet block
fn signet_sign_tx(
    block_version: bitcoin::block::Version,
    prev_blockhash: BlockHash,
    signet_merkle_root: WitnessMerkleNode,
    block_timestamp: u32,
    challenge_script: ScriptBuf,
) -> Transaction {
    let to_spend = signet_spend_tx(
        block_version,
        prev_blockhash,
        signet_merkle_root,
        block_timestamp,
        challenge_script,
    );
    Transaction {
        version: bitcoin::transaction::Version(0),
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::new(to_spend.compute_txid(), 0),
            script_sig: ScriptBuf::new(), // Will be filled with solution
            sequence: bitcoin::Sequence(0),
            witness: bitcoin::Witness::default(), // Will be filled with solution
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: Script::builder()
                .push_opcode(bitcoin::opcodes::all::OP_RETURN)
                .into_script(),
        }],
    }
}

/// Generate scriptSig and scriptWitness for a signet block
fn signet_sign_block<K>(
    block_version: bitcoin::block::Version,
    prev_blockhash: BlockHash,
    signet_merkle_root: WitnessMerkleNode,
    block_timestamp: u32,
    challenge_script: ScriptBuf,
    k: &K,
) -> Result<(ScriptBuf, bitcoin::Witness), bitcoin::psbt::SignError>
where
    K: bitcoin::psbt::GetKey,
{
    let tx = signet_sign_tx(
        block_version,
        prev_blockhash,
        signet_merkle_root,
        block_timestamp,
        challenge_script,
    );
    let mut psbt = bitcoin::psbt::Psbt::from_unsigned_tx(tx).unwrap();
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let _signing_keys_map: bitcoin::psbt::SigningKeysMap = psbt
        .sign(k, &secp)
        .map_err(|(_, mut errs)| errs.pop_first().unwrap().1)?;
    let mut tx = psbt.extract_tx_unchecked_fee_rate();
    let txin = tx.input.pop().unwrap();
    Ok((txin.script_sig, txin.witness))
}

const WITNESS_RESERVED_VALUE: [u8; 32] = [0; 32];

/// Add witness commitment output to the coinbase tx, and return a copy of the
/// witness commitment spk.
/// The coinbase tx should not include the witness commitment txout.
/// Signet challenge should be `Some` for signets, and `None` otherwise.
fn add_witness_commitment_output<SignetSigner>(
    coinbase_tx: &mut Transaction,
    block_version: bitcoin::block::Version,
    prev_blockhash: BlockHash,
    block_timestamp: u32,
    transactions: &[BlockTemplateTransaction],
    signet_challenge: Option<&Script>,
    signet_signer: &SignetSigner,
) -> Result<ScriptBuf, bitcoin::psbt::SignError>
where
    SignetSigner: bitcoin::psbt::GetKey,
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
    let mut witness_commitment_spk = {
        const WITNESS_COMMITMENT_HEADER: [u8; 4] = [0xaa, 0x21, 0xa9, 0xed];
        let mut push_bytes = PushBytesBuf::from(WITNESS_COMMITMENT_HEADER);
        let () = push_bytes
            .extend_from_slice(witness_commitment.as_byte_array())
            .unwrap();
        ScriptBuf::new_op_return(push_bytes)
    };
    if let Some(signet_challenge) = signet_challenge {
        const SIGNET_HEADER: [u8; 4] = [0xec, 0xc7, 0xda, 0xa2];
        let signet_merkle_root = {
            let mut modified_coinbase_tx = coinbase_tx.clone();
            modified_coinbase_tx.output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new_op_return(PushBytesBuf::from(
                    SIGNET_HEADER,
                )),
            });
            let hashes = std::iter::once(
                modified_coinbase_tx.compute_wtxid().to_raw_hash(),
            )
            .chain(transactions.iter().map(|tx| tx.hash.to_raw_hash()));
            merkle_tree::calculate_root(hashes)
                .map(WitnessMerkleNode::from_raw_hash)
                .unwrap()
        };
        let (script_sig, script_witness) = signet_sign_block(
            block_version,
            prev_blockhash,
            signet_merkle_root,
            block_timestamp,
            signet_challenge.to_owned(),
            signet_signer,
        )?;
        let mut push_bytes = PushBytesBuf::from(SIGNET_HEADER);
        push_bytes.extend_from_slice(script_sig.as_bytes()).unwrap();
        let script_witness_bytes =
            bitcoin::consensus::serialize(&script_witness);
        push_bytes.extend_from_slice(&script_witness_bytes).unwrap();
        witness_commitment_spk.push_slice(push_bytes);
    }
    coinbase_tx.output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: witness_commitment_spk.clone(),
    });
    Ok(witness_commitment_spk)
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
    #[error("Failed to sign signet block")]
    Signet(#[from] bitcoin::psbt::SignError),
}

/// Finalize coinbase tx, returning the coinbase tx and witness commitment spk
#[allow(clippy::too_many_arguments)]
fn finalize_coinbase_tx<SignetSigner>(
    coinbase_spk: ScriptBuf,
    block_version: bitcoin::block::Version,
    block_height: u32,
    prev_blockhash: BlockHash,
    block_timestamp: u32,
    network: Network,
    mut coinbase_txouts: Vec<TxOut>,
    transactions: &[BlockTemplateTransaction],
    signet_challenge: Option<&Script>,
    signet_signer: &SignetSigner,
) -> Result<(Transaction, ScriptBuf), FinalizeCoinbaseTxError>
where
    SignetSigner: bitcoin::psbt::GetKey,
{
    let bip34_height_script = bitcoin::blockdata::script::Builder::new()
        .push_int(block_height as i64)
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
            script_sig: bip34_height_script,
        }],
        output: coinbase_txouts,
    };
    let witness_commitment_spk = add_witness_commitment_output(
        &mut coinbase_tx,
        block_version,
        prev_blockhash,
        block_timestamp,
        transactions,
        signet_challenge,
        signet_signer,
    )?;
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
impl<BP, SignetSigner> RpcServer for Server<BP, SignetSigner>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
    SignetSigner: bitcoin::psbt::GetKey + Send + Sync + 'static,
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
                let tip_block = mempool.tip();
                let (coinbase_txn, block_txs, default_witness_commitment) =
                    if request.capabilities.contains("coinbasetxn") {
                        let (coinbase_txouts, block_txs) =
                            block_txs::<true, _>(enforcer, mempool)?;
                        let (coinbase_tx, witness_commitment_spk) =
                            finalize_coinbase_tx(
                                self.coinbase_spk.clone(),
                                version,
                                tip_block.height + 1,
                                tip_block.hash,
                                current_time_adjusted as u32,
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
                            block_txs::<false, _>(enforcer, mempool)?;
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
