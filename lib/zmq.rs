use std::time::Duration;

use bitcoin::{BlockHash, Txid, hashes::Hash as _, hex::DisplayHex as _};
use futures::{
    Stream, StreamExt, TryStreamExt as _,
    stream::{self, BoxStream},
};
use thiserror::Error;
use zeromq::{Socket as _, SocketRecv as _, ZmqError, ZmqMessage};

#[derive(Clone, Copy, Debug)]
pub enum BlockHashEvent {
    Connected,
    Disconnected,
}

#[derive(Clone, Copy, Debug)]
pub struct BlockHashMessage {
    pub block_hash: BlockHash,
    pub event: BlockHashEvent,
    pub zmq_seq: u32,
}

#[derive(Clone, Copy, Debug)]
pub enum TxHashEvent {
    /// Tx hash added to mempool
    Added,
    /// Tx hash removed from mempool for non-block inclusion reason
    Removed,
}

#[derive(Clone, Copy, Debug)]
pub struct TxHashMessage {
    pub txid: Txid,
    pub event: TxHashEvent,
    pub mempool_seq: u64,
    pub zmq_seq: u32,
}

#[derive(Clone, Copy, Debug)]
pub enum SequenceMessage {
    BlockHash(BlockHashMessage),
    TxHash(TxHashMessage),
}

impl SequenceMessage {
    fn mempool_seq(&self) -> Option<u64> {
        match self {
            Self::BlockHash { .. } => None,
            Self::TxHash(TxHashMessage { mempool_seq, .. }) => {
                Some(*mempool_seq)
            }
        }
    }

    fn zmq_seq(&self) -> u32 {
        match self {
            Self::BlockHash(BlockHashMessage { zmq_seq, .. })
            | Self::TxHash(TxHashMessage { zmq_seq, .. }) => *zmq_seq,
        }
    }
}

#[derive(Debug, Error)]
pub enum DeserializeSequenceMessageError {
    #[error("Missing hash (frame 1 bytes at index [0-31])")]
    MissingHash,
    #[error("Missing mempool sequence (frame 1 bytes at index [#33 - #40])")]
    MissingMempoolSequence,
    #[error("Missing message type (frame 1 index 32)")]
    MissingMessageType,
    #[error("Missing `sequence` prefix (frame 0 first 8 bytes)")]
    MissingPrefix,
    #[error("Missing ZMQ sequence (frame 2 first 4 bytes)")]
    MissingZmqSequence,
    #[error("Unknown message type: {0:x}")]
    UnknownMessageType(u8),
}

impl TryFrom<ZmqMessage> for SequenceMessage {
    type Error = DeserializeSequenceMessageError;

    fn try_from(msg: ZmqMessage) -> Result<Self, Self::Error> {
        parse_sequence_message(&msg.into_vec())
    }
}

fn parse_sequence_message<T: AsRef<[u8]>>(
    frames: &[T],
) -> Result<SequenceMessage, DeserializeSequenceMessageError> {
    use DeserializeSequenceMessageError as Error;
    let Some(b"sequence") = frames.first().map(|frame| frame.as_ref()) else {
        return Err(Error::MissingPrefix);
    };
    let Some((hash, rest)) = frames
        .get(1)
        .and_then(|frame| frame.as_ref().split_first_chunk())
    else {
        return Err(Error::MissingHash);
    };
    let mut hash = *hash;
    hash.reverse();
    let Some(([message_type], rest)) = rest.split_first_chunk() else {
        return Err(Error::MissingMessageType);
    };
    let Some((zmq_seq, _rest)) = frames
        .get(2)
        .and_then(|frame| frame.as_ref().split_first_chunk())
    else {
        return Err(Error::MissingZmqSequence);
    };
    let zmq_seq = u32::from_le_bytes(*zmq_seq);
    let res = match *message_type {
        b'C' => SequenceMessage::BlockHash(BlockHashMessage {
            block_hash: BlockHash::from_byte_array(hash),
            event: BlockHashEvent::Connected,
            zmq_seq,
        }),
        b'D' => SequenceMessage::BlockHash(BlockHashMessage {
            block_hash: BlockHash::from_byte_array(hash),
            event: BlockHashEvent::Disconnected,
            zmq_seq,
        }),
        b'A' => {
            let Some((mempool_seq, _rest)) = rest.split_first_chunk() else {
                return Err(Error::MissingMempoolSequence);
            };
            SequenceMessage::TxHash(TxHashMessage {
                txid: Txid::from_byte_array(hash),
                event: TxHashEvent::Added,
                mempool_seq: u64::from_le_bytes(*mempool_seq),
                zmq_seq,
            })
        }
        b'R' => {
            let Some((mempool_seq, _rest)) = rest.split_first_chunk() else {
                return Err(Error::MissingMempoolSequence);
            };
            SequenceMessage::TxHash(TxHashMessage {
                txid: Txid::from_byte_array(hash),
                event: TxHashEvent::Removed,
                mempool_seq: u64::from_le_bytes(*mempool_seq),
                zmq_seq,
            })
        }
        message_type => {
            return Err(Error::UnknownMessageType(message_type));
        }
    };
    Ok(res)
}

#[derive(Debug, Error)]
pub enum SequenceStreamError {
    #[error(
        "failed to deserialize ZMQ sequence message from frames {frames:?}"
    )]
    Deserialize {
        /// Hex-encoded frames, for diagnostics.
        frames: Vec<String>,
        #[source]
        source: DeserializeSequenceMessageError,
    },
    #[error(
        "Expected message with mempool sequence at least {min_next_seq}, but received {seq}"
    )]
    ExpectedMempoolSequenceAtLeast { min_next_seq: u64, seq: u64 },
    #[error("Missing message with mempool sequence {0}")]
    MissingMempoolSequence(u64),
    #[error("Missing message with zmq sequence {0}")]
    MissingZmqSequence(u32),
    #[error("failed to receive message on ZMQ `{topic}` stream")]
    Recv {
        topic: String,
        #[source]
        source: ZmqError,
    },
}

pub struct SequenceStream<'a>(
    BoxStream<'a, Result<SequenceMessage, SequenceStreamError>>,
);

impl Stream for SequenceStream<'_> {
    type Item = Result<SequenceMessage, SequenceStreamError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().0.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// Next mempool sequence
#[derive(Clone, Copy, Debug)]
enum NextMempoolSeq {
    /// After a block (dis)connect event, next mempool seq is incremented by
    /// the number of txs added/removed from mempool by block (dis)connect
    AtLeast(u64),
    Equal(u64),
}

fn check_mempool_seq(
    next_mempool_seq: &mut Option<NextMempoolSeq>,
    msg: SequenceMessage,
) -> Result<Option<SequenceMessage>, SequenceStreamError> {
    match (*next_mempool_seq, msg.mempool_seq()) {
        (None, Some(mempool_seq)) => {
            *next_mempool_seq = Some(NextMempoolSeq::Equal(mempool_seq + 1));
            Ok(Some(msg))
        }
        (Some(NextMempoolSeq::AtLeast(min_next_seq)), Some(mempool_seq)) => {
            // No duplicate message is possible, since we know that the last
            // message must have been a block event
            if mempool_seq >= min_next_seq {
                *next_mempool_seq =
                    Some(NextMempoolSeq::Equal(mempool_seq + 1));
                Ok(Some(msg))
            } else {
                let err = SequenceStreamError::ExpectedMempoolSequenceAtLeast {
                    min_next_seq,
                    seq: mempool_seq,
                };
                Err(err)
            }
        }
        (Some(NextMempoolSeq::Equal(next_seq)), Some(mempool_seq)) => {
            if mempool_seq + 1 == next_seq {
                // Ignore duplicates
                Ok(None)
            } else if mempool_seq == next_seq {
                *next_mempool_seq =
                    Some(NextMempoolSeq::Equal(mempool_seq + 1));
                Ok(Some(msg))
            } else if mempool_seq > next_seq {
                // A forward jump is NOT evidence of a dropped message: a block
                // connect removes mined txs, each of which advances the node's
                // mempool sequence with no message published, and bitcoind
                // publishes removals that follow the block (e.g. conflict
                // evictions) BEFORE the block's own `C` message. Waiting for
                // that `C` to move us into `AtLeast` would be too late — the
                // jump reaches us first. Dropped messages are detected by the
                // per-topic zmq sequence in `check_zmq_seq`, which is
                // contiguous across every published message; so accept the
                // jump and re-anchor. (bip300301_enforcer#610)
                tracing::debug!(
                    expected = next_seq,
                    received = mempool_seq,
                    "mempool sequence jumped forward; treating as block-removed \
                     txs, not a dropped message",
                );
                *next_mempool_seq =
                    Some(NextMempoolSeq::Equal(mempool_seq + 1));
                Ok(Some(msg))
            } else {
                let err = SequenceStreamError::MissingMempoolSequence(next_seq);
                Err(err)
            }
        }
        (None | Some(NextMempoolSeq::AtLeast(_)), None) => Ok(Some(msg)),
        (Some(NextMempoolSeq::Equal(next_seq)), None) => {
            *next_mempool_seq = Some(NextMempoolSeq::AtLeast(next_seq));
            Ok(Some(msg))
        }
    }
}

fn check_zmq_seq(
    next_zmq_seq: &mut Option<u32>,
    msg: SequenceMessage,
) -> Result<Option<SequenceMessage>, SequenceStreamError> {
    let zmq_seq = msg.zmq_seq();
    match next_zmq_seq {
        None => {
            *next_zmq_seq = Some(zmq_seq + 1);
            Ok(Some(msg))
        }
        Some(next_seq) => {
            if zmq_seq + 1 == *next_seq {
                // Ignore duplicates
                Ok(None)
            } else if zmq_seq == *next_seq {
                *next_seq += 1;
                Ok(Some(msg))
            } else {
                let err = SequenceStreamError::MissingZmqSequence(*next_seq);
                Err(err)
            }
        }
    }
}

fn check_seq_numbers(
    next_mempool_seq: &mut Option<NextMempoolSeq>,
    next_zmq_seq: &mut Option<u32>,
    msg: SequenceMessage,
) -> Result<Option<SequenceMessage>, SequenceStreamError> {
    let Some(msg) = check_mempool_seq(next_mempool_seq, msg)? else {
        return Ok(None);
    };
    check_zmq_seq(next_zmq_seq, msg)
}

#[derive(Debug, Error)]
pub enum SubscribeSequenceError {
    #[error(
        "ZMQ connection timeout after {timeout:?} connecting to `{target}`"
    )]
    Timeout {
        target: String,
        timeout: Duration,
        #[source]
        source: tokio::time::error::Elapsed,
    },
    #[error("failed to connect to ZMQ server at `{target}`")]
    Connect {
        target: String,
        #[source]
        source: ZmqError,
    },
    #[error("failed to subscribe to ZMQ topic `{topic}`")]
    Subscribe {
        topic: String,
        #[source]
        source: ZmqError,
    },
}

/// Subscribe to ZMQ sequence stream.
/// Sequence numbers are checked, although mempool sequence numbers can only
/// be partially checked, since block (dis)connect events may increment
/// mempool sequence numbers in a manner that cannot be determined from
/// block event messages alone.
#[tracing::instrument]
pub async fn subscribe_sequence<'a>(
    zmq_addr_sequence: &str,
) -> Result<SequenceStream<'a>, SubscribeSequenceError> {
    tracing::debug!("Attempting to connect to ZMQ server...");
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
    let mut socket = zeromq::SubSocket::new();
    tokio::time::timeout(CONNECTION_TIMEOUT, socket.connect(zmq_addr_sequence))
        .await
        .map_err(|source| SubscribeSequenceError::Timeout {
            target: zmq_addr_sequence.to_string(),
            timeout: CONNECTION_TIMEOUT,
            source,
        })?
        .map_err(|source| SubscribeSequenceError::Connect {
            target: zmq_addr_sequence.to_string(),
            source,
        })?;
    tracing::info!("Connected to ZMQ server");

    const TOPIC: &str = "sequence";
    tracing::debug!("Attempting to subscribe to `{TOPIC}` topic...");

    socket.subscribe(TOPIC).await.map_err(|source| {
        SubscribeSequenceError::Subscribe {
            topic: TOPIC.to_string(),
            source,
        }
    })?;
    tracing::info!("Subscribed to `{TOPIC}`");
    let inner = stream::try_unfold(socket, |mut socket| async {
        let raw = socket.recv().await.map_err(|source| {
            SequenceStreamError::Recv {
                topic: TOPIC.to_string(),
                source,
            }
        })?;
        let frames = raw.into_vec();
        let msg = parse_sequence_message(&frames).map_err(|source| {
            SequenceStreamError::Deserialize {
                frames: frames
                    .iter()
                    .map(|frame| frame.as_ref().to_lower_hex_string())
                    .collect(),
                source,
            }
        })?;
        Ok(Some((msg, socket)))
    })
    .try_filter_map({
        let mut next_mempool_seq: Option<NextMempoolSeq> = None;
        let mut next_zmq_seq: Option<u32> = None;
        move |sequence_msg| {
            let res = check_seq_numbers(
                &mut next_mempool_seq,
                &mut next_zmq_seq,
                sequence_msg,
            );
            futures::future::ready(res)
        }
    })
    .boxed();
    Ok(SequenceStream(inner))
}

#[cfg(test)]
mod tests {
    use bitcoin::{BlockHash, Txid, hashes::Hash as _};

    use super::*;

    fn tx(
        event: TxHashEvent,
        mempool_seq: u64,
        zmq_seq: u32,
    ) -> SequenceMessage {
        SequenceMessage::TxHash(TxHashMessage {
            txid: Txid::all_zeros(),
            event,
            mempool_seq,
            zmq_seq,
        })
    }

    fn block(event: BlockHashEvent, zmq_seq: u32) -> SequenceMessage {
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash: BlockHash::all_zeros(),
            event,
            zmq_seq,
        })
    }

    /// Replays the exact publish order captured from a live bitcoind at a
    /// block connect (bip300301_enforcer#610): the removals that follow the
    /// block are published, numbered past the silently block-removed txs,
    /// BEFORE the block's own `C` message. The zmq sequence is contiguous —
    /// nothing was dropped — so the stream must not error.
    #[test]
    fn forward_mempool_seq_jump_before_block_connect_is_not_a_drop() {
        let mut next_mempool = None;
        let mut next_zmq = None;
        let msgs = [
            tx(TxHashEvent::Added, 66956, 34040),
            tx(TxHashEvent::Removed, 67431, 34041),
            block(BlockHashEvent::Connected, 34042),
            tx(TxHashEvent::Added, 67580, 34043),
            tx(TxHashEvent::Added, 67581, 34044),
        ];
        for msg in msgs {
            let res = check_seq_numbers(&mut next_mempool, &mut next_zmq, msg);
            assert!(
                res.is_ok(),
                "stream must accept the block-removal jump: {res:?}"
            );
            assert!(res.unwrap().is_some(), "no message here is a duplicate");
        }
    }

    /// A real drop still surfaces through the zmq sequence.
    #[test]
    fn zmq_seq_gap_is_still_detected() {
        let mut next_mempool = None;
        let mut next_zmq = None;
        assert!(
            check_seq_numbers(
                &mut next_mempool,
                &mut next_zmq,
                tx(TxHashEvent::Added, 10, 100)
            )
            .is_ok()
        );
        let res = check_seq_numbers(
            &mut next_mempool,
            &mut next_zmq,
            tx(TxHashEvent::Added, 11, 102),
        );
        assert!(
            matches!(res, Err(SequenceStreamError::MissingZmqSequence(101))),
            "{res:?}"
        );
    }

    /// Duplicates (same mempool seq re-sent) are still ignored, and a
    /// backwards mempool sequence is still an error.
    #[test]
    fn duplicate_and_backwards_mempool_seq_behaviour_unchanged() {
        let mut next_mempool = None;
        let mut next_zmq = None;
        assert!(
            check_seq_numbers(
                &mut next_mempool,
                &mut next_zmq,
                tx(TxHashEvent::Added, 10, 1)
            )
            .unwrap()
            .is_some()
        );
        assert!(
            check_seq_numbers(
                &mut next_mempool,
                &mut next_zmq,
                tx(TxHashEvent::Added, 10, 2)
            )
            .unwrap()
            .is_none()
        );
        let res = check_seq_numbers(
            &mut next_mempool,
            &mut next_zmq,
            tx(TxHashEvent::Added, 5, 3),
        );
        assert!(
            matches!(res, Err(SequenceStreamError::MissingMempoolSequence(11))),
            "{res:?}"
        );
    }
}
