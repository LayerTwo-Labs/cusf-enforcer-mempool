//! Best-effort reader for Core's `mempool.dat`.
//!
//! This is a fast path for the initial mempool sync. Every failure mode
//! here is expected to be survivable
//!
//! Format (Core v28+ writes version 2. v1 is the same without the XOR key):
//!
//! ```text
//! u64                  version
//! compact_size + bytes  XOR key         (version 2 only)
//! --- everything below is XOR-obfuscated in version 2 ---
//! u64                  number of transactions
//! repeated:
//!     CTransaction     (consensus encoding, with witness)
//!     i64              entry time
//!     i64              fee delta
//! ...                  mapDeltas, unbroadcast set (not read)
//! ```
//!
//! The XOR key is indexed by absolute file offset, not by offset within the
//! obfuscated region.
//!
//! This format is internal to Core and carries no compatibility promise. It has
//! already changed once (v28 added the key).

use std::collections::HashMap;
use std::path::Path;

use bitcoin::consensus::Decodable;
use bitcoin::{Transaction, Txid};

/// Smallest possible serialized entry: a minimal transaction plus the two
/// `i64`s that follow it. Used to bound the up-front allocation against a
/// corrupt count
const MIN_ENTRY_BYTES: usize = 60 + 8 + 8;

const VERSION_NO_XOR: u64 = 1;
const VERSION_XOR: u64 = 2;

#[derive(Debug, thiserror::Error)]
pub enum ReadMempoolDatError {
    #[error("failed to read `{path}`")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("file is too short to be a mempool dump ({len} bytes)")]
    TooShort { len: usize },
    #[error("unsupported mempool.dat version {version}")]
    UnsupportedVersion { version: u64 },
    #[error("XOR key length {len} is not 8")]
    BadXorKeyLen { len: usize },
}

#[derive(Debug)]
pub struct MempoolDat {
    pub txs: HashMap<Txid, Transaction>,
    /// Number of entries the header claimed.
    pub declared: usize,
    /// Set when decoding stopped early, with the entry index it stopped at.
    pub truncated_at: Option<usize>,
}

/// De-obfuscate in place. `start_offset` is the file position of `buf[0]`.
fn apply_xor(buf: &mut [u8], key: &[u8; 8], start_offset: usize) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b ^= key[(start_offset + i) % 8];
    }
}

/// Read and decode `mempool.dat`, recovering as many transactions as possible.
///
/// Returns `Err` only when nothing at all could be made of the file. A
/// partially readable dump yields `Ok` with `truncated_at` set.
pub fn read_mempool_dat(
    path: &Path,
) -> Result<MempoolDat, ReadMempoolDatError> {
    // Read the whole file: it is on the order of 100MB on a full mainnet mempool, and the
    // alternative (buffered reads through the XOR) buys little for the
    // complexity.
    let mut buf =
        std::fs::read(path).map_err(|source| ReadMempoolDatError::Io {
            path: path.display().to_string(),
            source,
        })?;

    if buf.len() < 8 {
        return Err(ReadMempoolDatError::TooShort { len: buf.len() });
    }
    let version = u64::from_le_bytes(
        buf[0..8].try_into().expect("slice is exactly 8 bytes"),
    );

    let (body_start, key) = match version {
        VERSION_NO_XOR => (8usize, [0u8; 8]),
        VERSION_XOR => {
            // compact_size length prefix, then the key itself
            let key_len = *buf
                .get(8)
                .ok_or(ReadMempoolDatError::TooShort { len: buf.len() })?
                as usize;
            if key_len != 8 {
                return Err(ReadMempoolDatError::BadXorKeyLen { len: key_len });
            }
            let key: [u8; 8] = buf
                .get(9..17)
                .ok_or(ReadMempoolDatError::TooShort { len: buf.len() })?
                .try_into()
                .expect("slice is exactly 8 bytes");
            (17usize, key)
        }
        version => {
            return Err(ReadMempoolDatError::UnsupportedVersion { version });
        }
    };

    if buf.len() < body_start + 8 {
        return Err(ReadMempoolDatError::TooShort { len: buf.len() });
    }
    apply_xor(&mut buf[body_start..], &key, body_start);

    let declared = u64::from_le_bytes(
        buf[body_start..body_start + 8]
            .try_into()
            .expect("slice is exactly 8 bytes"),
    );
    let mut cursor = &buf[body_start + 8..];

    let ceiling = cursor.len() / MIN_ENTRY_BYTES;
    let capacity = (declared as usize).min(ceiling);
    let mut txs = HashMap::with_capacity(capacity);

    let declared = declared as usize;
    let mut truncated_at = None;
    for index in 0..declared {
        // Decoding walks `cursor` forward. A failure leaves it in an
        // indeterminate position, so we abort
        let tx = match Transaction::consensus_decode(&mut cursor) {
            Ok(tx) => tx,
            Err(err) => {
                tracing::debug!(
                    %index,
                    %declared,
                    "mempool.dat: stopping at undecodable transaction: {err}"
                );
                truncated_at = Some(index);
                break;
            }
        };
        // The two trailing i64s are not needed  but they must be
        // consumed to stay aligned for the next entry.
        if i64::consensus_decode(&mut cursor).is_err()
            || i64::consensus_decode(&mut cursor).is_err()
        {
            tracing::debug!(
                %index,
                %declared,
                "mempool.dat: truncated entry metadata"
            );
            truncated_at = Some(index);
            break;
        }
        txs.insert(tx.compute_txid(), tx);
    }

    Ok(MempoolDat {
        txs,
        declared,
        truncated_at,
    })
}

#[cfg(test)]
mod tests {
    use bitcoin::consensus::Encodable as _;

    use super::{
        MIN_ENTRY_BYTES, ReadMempoolDatError, apply_xor, read_mempool_dat,
    };

    /// A minimal but structurally valid transaction.
    fn dummy_tx(lock_time: u32) -> bitcoin::Transaction {
        bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::from_consensus(lock_time),
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        }
    }

    /// Build a dump the way Core does
    fn build_dump(
        version: u64,
        key: Option<[u8; 8]>,
        txs: &[bitcoin::Transaction],
        declared_override: Option<u64>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&version.to_le_bytes());
        if let Some(key) = key {
            out.push(8u8);
            out.extend_from_slice(&key);
        }
        let body_start = out.len();
        out.extend_from_slice(
            &declared_override.unwrap_or(txs.len() as u64).to_le_bytes(),
        );
        for tx in txs {
            tx.consensus_encode(&mut out).unwrap();
            out.extend_from_slice(&0i64.to_le_bytes());
            out.extend_from_slice(&0i64.to_le_bytes());
        }
        if let Some(key) = key {
            apply_xor(&mut out[body_start..], &key, body_start);
        }
        out
    }

    fn write_temp(bytes: &[u8], name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cusf-mempool-dat-test-{name}-{}",
            std::process::id()
        ));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// The masking is only proven correct by a round trip: an off-by-one in the
    /// XOR index still decodes the tx count when the region is 8-aligned.
    #[test]
    fn round_trips_v2_with_xor() {
        let txs: Vec<_> = (0..8).map(dummy_tx).collect();
        let key = [0x4f, 0xef, 0xe4, 0xbb, 0xd2, 0x8c, 0x72, 0x5e];
        let path = write_temp(&build_dump(2, Some(key), &txs, None), "v2");
        let parsed = read_mempool_dat(&path).unwrap();
        assert_eq!(parsed.declared, 8);
        assert_eq!(parsed.txs.len(), 8);
        assert!(parsed.truncated_at.is_none());
        for tx in &txs {
            assert_eq!(parsed.txs.get(&tx.compute_txid()), Some(tx));
        }
        drop(std::fs::remove_file(&path));
    }

    #[test]
    fn round_trips_v1_without_xor() {
        let txs: Vec<_> = (0..4).map(dummy_tx).collect();
        let path = write_temp(&build_dump(1, None, &txs, None), "v1");
        let parsed = read_mempool_dat(&path).unwrap();
        assert_eq!(parsed.txs.len(), 4);
        assert!(parsed.truncated_at.is_none());
        drop(std::fs::remove_file(&path));
    }

    /// A dump being rewritten underneath us is the expected case, not an error:
    /// return what was recovered so the caller can still skip that much RPC.
    #[test]
    fn truncated_file_yields_partial_results() {
        let txs: Vec<_> = (0..10).map(dummy_tx).collect();
        let key = [1, 2, 3, 4, 5, 6, 7, 8];
        let mut dump = build_dump(2, Some(key), &txs, None);
        dump.truncate(dump.len() / 2);
        let path = write_temp(&dump, "truncated");
        let parsed = read_mempool_dat(&path).unwrap();
        assert!(parsed.truncated_at.is_some());
        assert!(
            !parsed.txs.is_empty() && parsed.txs.len() < 10,
            "expected some but not all txs, got {}",
            parsed.txs.len()
        );
        drop(std::fs::remove_file(&path));
    }

    /// A corrupt count must not be used to size an allocation. Without the
    /// ceiling this aborts the process instead of failing the test.
    #[test]
    fn absurd_declared_count_does_not_allocate() {
        let txs: Vec<_> = (0..2).map(dummy_tx).collect();
        let key = [9; 8];
        let dump = build_dump(2, Some(key), &txs, Some(u64::MAX));
        let path = write_temp(&dump, "absurd");
        let parsed = read_mempool_dat(&path).unwrap();
        assert_eq!(parsed.declared, u64::MAX as usize);
        assert_eq!(parsed.txs.len(), 2);
        assert_eq!(parsed.truncated_at, Some(2));
        drop(std::fs::remove_file(&path));
    }

    #[test]
    fn unknown_version_is_rejected_not_guessed() {
        let path = write_temp(&build_dump(3, None, &[], None), "v3");
        assert!(matches!(
            read_mempool_dat(&path),
            Err(ReadMempoolDatError::UnsupportedVersion { version: 3 })
        ));
        drop(std::fs::remove_file(&path));
    }

    #[test]
    fn missing_file_is_an_error_not_a_panic() {
        let path = std::env::temp_dir().join("cusf-mempool-dat-does-not-exist");
        drop(std::fs::remove_file(&path));
        assert!(matches!(
            read_mempool_dat(&path),
            Err(ReadMempoolDatError::Io { .. })
        ));
    }

    /// Random bytes must never panic, however they are framed.
    #[test]
    fn garbage_never_panics() {
        for len in [0usize, 1, 7, 8, 9, 16, 17, 24, 64, MIN_ENTRY_BYTES] {
            let garbage: Vec<u8> =
                (0..len).map(|i| (i as u8).wrapping_mul(37)).collect();
            let path = write_temp(&garbage, &format!("garbage-{len}"));
            // Either outcome is fine; a panic or abort is not.
            drop(read_mempool_dat(&path));
            drop(std::fs::remove_file(&path));
        }
    }
}
