//! Seeding the initial sync from Core's `mempool.dat` is an optimization, and
//! must behave like one: the resulting mempool has to be identical whether the
//! dump was usable, garbage, or absent.
//!
//! Three syncs run against one node with the same mempool:
//!   1. the real dump         -> fast path, expected to seed everything
//!   2. a file full of garbage -> unreadable, must fall back
//!   3. a path that does not exist -> must fall back
//!
//! All three must agree

use std::time::Duration;

use std::collections::HashSet;

use bitcoin::Txid;
use bitcoin_jsonrpsee::jsonrpsee::{core::client::ClientT as _, rpc_params};

use crate::{
    mock_enforcer::MockEnforcer,
    setup::{
        Directories, RegtestNode, local_mempool_txids, start_mempool_sync,
        wait_for_local_mempool,
    },
    util::{BinPaths, submit_tx, wait_for_mempool_pred},
};

/// How many txs to put in the mempool before dumping.
const TXS: usize = 12;

/// Run an init-sync seeded from `dat`, wait for it to take in every tx that
/// bitcoind has, and return the resulting mempool.
async fn synced_txids(
    node: &RegtestNode,
    dat: Option<std::path::PathBuf>,
    expected: &[Txid],
    label: &str,
) -> anyhow::Result<HashSet<Txid>> {
    let (mempool_sync, task_errors) =
        start_mempool_sync(node, MockEnforcer::new(), dat).await?;
    let expected: HashSet<Txid> = expected.iter().copied().collect();
    wait_for_local_mempool(
        &mempool_sync,
        &task_errors,
        Duration::from_secs(30),
        |txids| expected.is_subset(txids),
        label,
    )
    .await?;
    Ok(local_mempool_txids(&mempool_sync).await)
}

pub async fn test_mempool_dat_fast_path(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node = RegtestNode::new(&bin_paths, directories).await?;

    let mut submitted = Vec::new();
    for _ in 0..TXS {
        submitted.push(submit_tx(&node.rpc_client, 50_000).await?);
    }
    let last = *submitted.last().expect("submitted at least one tx");
    wait_for_mempool_pred(
        &node.rpc_client,
        Duration::from_secs(10),
        move |t| t.contains(&last),
        "seed txs in bitcoind mempool",
    )
    .await?;

    // Core only writes the dump on shutdown or on request, so an enforcer using
    // this path has to ask for it. Doing so here also makes the fixture match
    // the mempool exactly, which is what run 1 asserts.
    let _saved: serde_json::Value = node
        .rpc_client
        .request("savemempool", rpc_params![])
        .await?;

    let dat_path = node.bitcoind.data_dir.join("regtest").join("mempool.dat");
    anyhow::ensure!(
        dat_path.exists(),
        "expected a dump at {} after savemempool",
        dat_path.display()
    );

    // 1. The real dump.
    let from_dat =
        synced_txids(&node, Some(dat_path.clone()), &submitted, "valid dump")
            .await?;

    // 2. Garbage. Bytes that are not a dump at all — a plausible outcome if the
    //    file is mid-write, or written by a Core whose format we do not know.
    let garbage_path = node.bitcoind.data_dir.join("mempool.dat.garbage");
    std::fs::write(&garbage_path, vec![0xABu8; 4096])?;
    let from_garbage =
        synced_txids(&node, Some(garbage_path), &submitted, "garbage dump")
            .await?;

    // 3. No file. The default for any enforcer not sharing a filesystem with
    //    its node.
    let missing_path = node.bitcoind.data_dir.join("mempool.dat.absent");
    let from_missing =
        synced_txids(&node, Some(missing_path), &submitted, "absent dump")
            .await?;

    // The RPC-only runs are the reference: whatever they produce is what the
    // fast path must reproduce exactly.
    anyhow::ensure!(
        from_garbage == from_missing,
        "the two fallback runs disagreed: {} vs {} txs",
        from_garbage.len(),
        from_missing.len(),
    );
    anyhow::ensure!(
        from_dat == from_garbage,
        "seeding from mempool.dat changed the synced mempool: \
         {} txs from the dump vs {} via RPC; \
         only in dump: {:?}, only via RPC: {:?}",
        from_dat.len(),
        from_garbage.len(),
        from_dat.difference(&from_garbage).collect::<Vec<_>>(),
        from_garbage.difference(&from_dat).collect::<Vec<_>>(),
    );

    // Guard against the whole thing passing vacuously on an empty mempool.
    anyhow::ensure!(
        from_dat.len() >= TXS,
        "expected at least {TXS} txs to be synced, got {}",
        from_dat.len()
    );

    Ok(())
}
