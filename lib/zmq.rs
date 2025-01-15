use bitcoin::{hashes::Hash as _, BlockHash, Txid};
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt as _,
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
        let msgs = &msg.into_vec();
        let Some(b"sequence") = msgs.first().map(|msg| &**msg) else {
            return Err(Self::Error::MissingPrefix);
        };
        let Some((hash, rest)) =
            msgs.get(1).and_then(|msg| msg.split_first_chunk())
        else {
            return Err(Self::Error::MissingHash);
        };
        let mut hash = *hash;
        hash.reverse();
        let Some(([message_type], rest)) = rest.split_first_chunk() else {
            return Err(Self::Error::MissingMessageType);
        };
        let Some((zmq_seq, _rest)) =
            msgs.get(2).and_then(|msg| msg.split_first_chunk())
        else {
            return Err(Self::Error::MissingZmqSequence);
        };
        let zmq_seq = u32::from_le_bytes(*zmq_seq);
        let res = match *message_type {
            b'C' => Self::BlockHash(BlockHashMessage {
                block_hash: BlockHash::from_byte_array(hash),
                event: BlockHashEvent::Connected,
                zmq_seq,
            }),
            b'D' => Self::BlockHash(BlockHashMessage {
                block_hash: BlockHash::from_byte_array(hash),
                event: BlockHashEvent::Disconnected,
                zmq_seq,
            }),
            b'A' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                Self::TxHash(TxHashMessage {
                    txid: Txid::from_byte_array(hash),
                    event: TxHashEvent::Added,
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                    zmq_seq,
                })
            }
            b'R' => {
                let Some((mempool_seq, _rest)) = rest.split_first_chunk()
                else {
                    return Err(Self::Error::MissingMempoolSequence);
                };
                SequenceMessage::TxHash(TxHashMessage {
                    txid: Txid::from_byte_array(hash),
                    event: TxHashEvent::Removed,
                    mempool_seq: u64::from_le_bytes(*mempool_seq),
                    zmq_seq,
                })
            }
            message_type => {
                return Err(Self::Error::UnknownMessageType(message_type))
            }
        };
        Ok(res)
    }
}

#[derive(Debug, Error)]
pub enum SequenceStreamError {
    #[error("Error deserializing message")]
    Deserialize(#[from] DeserializeSequenceMessageError),
    #[error("Expected message with mempool sequence at least {min_next_seq}, but received {seq}")]
    ExpectedMempoolSequenceAtLeast { min_next_seq: u64, seq: u64 },
    #[error("Missing message with mempool sequence {0}")]
    MissingMempoolSequence(u64),
    #[error("Missing message with zmq sequence {0}")]
    MissingZmqSequence(u32),
    #[error("ZMQ error")]
    Zmq(#[from] ZmqError),
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

/// Subscribe to ZMQ sequence stream.
/// Sequence numbers are checked, although mempool sequence numbers can only
/// be partially checked, since block (dis)connect events may increment
/// mempool sequence numbers in a manner that cannot be determined from
/// block event messages alone.
#[tracing::instrument]
pub async fn subscribe_sequence<'a>(
    zmq_addr_sequence: &str,
) -> Result<SequenceStream<'a>, ZmqError> {
    tracing::debug!("Attempting to connect to ZMQ server...");
    let mut socket = zeromq::SubSocket::new();
    socket.connect(zmq_addr_sequence).await?;
    tracing::info!("Connected to ZMQ server");
    tracing::debug!("Attempting to subscribe to `sequence` topic...");
    socket.subscribe("sequence").await?;
    tracing::info!("Subscribed to `sequence`");
    let inner = stream::try_unfold(socket, |mut socket| async {
        let msg: SequenceMessage = socket.recv().await?.try_into()?;
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
