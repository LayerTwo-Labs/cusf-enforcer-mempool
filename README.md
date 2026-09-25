# CUSF Enforcer Mempool

## Build

* Install dependencies (rustup)
* Clone this repository
* Build with `cargo build` or `cargo build --release`

## Configure Bitcoin node
* Must use a version of Bitcoin Core more recent than `75118a608fc22a57567743000d636bc1f969f748`.
* RPC server MUST be enabled.
* ZMQ sequence publishing MUST be enabled.
* `txindex` MUST be enabled.
## Slipstream

Off by default. Enable it with `Server::with_slipstream` (or
`--enable-slipstream` on the demo app) to accept txs submitted straight to this
mempool. A slipstream tx is never relayed and never enters the node's mempool:
it competes for template inclusion by fee rate like any other tx, and the node's
relay policy does not apply to it. Consensus validity does: before it is
accepted, the node checks a block containing the tx and its in-mempool ancestors
as a `getblocktemplate` proposal.

The GBT server has no authentication, so anyone who can reach it can submit.
Serve it only where that is intended.

| Method | Result |
|---|---|
| `submitslipstreamtx <hex>` | `{txid, wtxid, accepted, reject_reason?, already_present, fee_sat?, vsize, weight, sigop_cost?, conflicts_with}`. A refused tx is `accepted: false` with a reason, in the style of `testmempoolaccept`. |
| `getslipstreamtx <txid>` | `{status: "pending", ...}`, `{status: "removed", reason, ...}` or `{status: "unknown", txid}` |
| `listslipstreamtxs` | Pending slipstream txs, oldest first |
| `removeslipstreamtx <txid>` | Withdraws the tx and any slipstream txs spending it; returns the txids removed |

A slipstream tx leaves the mempool for one of these `reason`s:

* `mined`: confirmed, in `block_hash`.
* `conflict_mined`: `block_hash` confirmed `spent_by`, which spends one of its inputs.
* `parent_removed`: an in-mempool `parent` left the node's mempool without being mined.
* `rejected_by_enforcer`: the enforcer's rules removed it at a block connect or disconnect.
* `reorged`: `block_hash` was disconnected. A reorg can leave a tx invalid, and the
  node never re-checks a tx it does not have, so every slipstream tx is evicted.
  Submit it again.
* `withdrawn`: `removeslipstreamtx`.

Removed txs are remembered for the last 10,000 removals, and nothing is
persisted: after a restart, every slipstream tx must be submitted again.

Limits are set with `SlipstreamConfig`: at most 1,000 txs, 1,000,000 wu per tx,
and 16,000 sigop cost per tx and across all of them. Template selection does not
count sigops, so the pool-wide budget is what keeps slipstream txs from taking a
block past its limit.
