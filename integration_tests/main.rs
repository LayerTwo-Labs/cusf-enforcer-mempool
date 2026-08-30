use std::time::Duration;

use clap::Parser;
use cusf_enforcer_mempool_integration_tests::{
    setup::{Directories, TestSetup},
    test_accept_tx_paths, test_block_connect_smoke,
    test_connect_block_deprioritizes_removed_txs,
    test_disconnect_through_sync_tip, test_disconnect_unrejects_removed_txs,
    test_double_insert_after_reorg, test_enforcer_rejection_during_reorg,
    test_gbt_long_poll, test_invalid_block_during_initial_sync,
    test_mempool_dat_fast_path, test_rbf_removed_for_absent_tx,
    test_rejected_block_disconnect, test_reorg_re_inserts_tx,
    test_tx_replaced_during_fetch,
    util::{
        BinPaths, TestFailure, TestFailureCollector, display_timing_summary,
        record_test_timing,
    },
};
use libtest_mimic::{Arguments, Trial};
use tokio_util::task::AbortOnDropHandle;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter as tracing_filter, layer::SubscriberExt, util::SubscriberInitExt,
};

const PER_TEST_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Parser)]
struct Cli {
    /// Stream test-harness logs at this level while tests run: off, error,
    /// warn, info, debug, or trace. Off by default; the progress lines, the
    /// pass/fail summary, and per-failure log dumps are always shown.
    #[arg(long, default_value = "off", value_name = "LEVEL")]
    log_level: LevelFilter,
    #[command(flatten)]
    test_args: Arguments,
}

/// Saturating predecessor of a log level — for setting a quieter default on
/// noisy upstream crates while keeping `log_level` for the integration tests.
fn saturating_pred_level(log_level: tracing::Level) -> tracing::Level {
    match log_level {
        tracing::Level::TRACE => tracing::Level::DEBUG,
        tracing::Level::DEBUG => tracing::Level::INFO,
        tracing::Level::INFO => tracing::Level::WARN,
        tracing::Level::WARN => tracing::Level::ERROR,
        tracing::Level::ERROR => tracing::Level::ERROR,
    }
}

fn targets_directive_str<'a>(
    targets: impl IntoIterator<Item = (&'a str, tracing::Level)>,
) -> String {
    targets
        .into_iter()
        .map(|(target, level)| {
            let level = level.as_str().to_ascii_lowercase();
            if target.is_empty() {
                level
            } else {
                format!("{target}={level}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

// Configure logger. `log_level` gates streaming of harness log events: `off`
// (the default) streams nothing — the progress lines, results, and per-failure
// dumps are printed directly, not via tracing — and any other level streams
// the harness crates at that level (deps one level lower).
fn set_tracing_subscriber(log_level: LevelFilter) -> anyhow::Result<()> {
    let Some(level) = log_level.into_level() else {
        return Ok(());
    };
    let targets_filter = {
        let defaults = targets_directive_str([
            ("", saturating_pred_level(level)),
            ("cusf_enforcer_mempool", level),
            ("cusf_enforcer_mempool_integration_tests", level),
        ]);
        let directives =
            match std::env::var(tracing_filter::EnvFilter::DEFAULT_ENV) {
                Ok(env) => format!("{defaults},{env}"),
                Err(std::env::VarError::NotPresent) => defaults,
                Err(err) => return Err(err.into()),
            };
        tracing_filter::EnvFilter::builder().parse(directives)?
    };
    tracing_subscriber::registry()
        .with(targets_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_target(true)
                .with_line_number(true)
                .with_writer(std::io::stderr),
        )
        .try_init()
        .map_err(|err| {
            anyhow::anyhow!("setting tracing subscriber failed: {err:#}")
        })
}

/// Build a `libtest-mimic` Trial. Creates a per-test `Directories`, passes
/// it to `f`, and on failure records the test name + bitcoind log dir in
/// `collector` for the end-of-run summary.
fn make_trial<F, Fut>(
    name: &str,
    bin_paths: BinPaths,
    collector: TestFailureCollector,
    rt_handle: tokio::runtime::Handle,
    f: F,
) -> Trial
where
    F: FnOnce(BinPaths, Directories) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let name = name.to_owned();
    let span_name = name.clone();
    Trial::test(name.clone(), move || {
        let test_name = name.clone();
        let timing_name = name.clone();
        let span_name = span_name.clone();
        let started = std::time::Instant::now();
        let outcome = std::thread::spawn(move || {
            rt_handle.block_on(async move {
                let span = tracing::info_span!("test", name = %span_name);
                let _entered = span.enter();
                let dirs = Directories::new(&test_name)?;
                let bitcoind_dir = dirs.bitcoind_dir.clone();
                let handle =
                    AbortOnDropHandle::new(tokio::spawn(f(bin_paths, dirs)));
                let result = match tokio::time::timeout(
                    PER_TEST_TIMEOUT,
                    handle,
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(join_err)) => Err(anyhow::Error::new(join_err)
                        .context("test task failed")),
                    Err(_) => Err(anyhow::anyhow!(
                        "test timed out after {PER_TEST_TIMEOUT:?}"
                    )),
                };
                if let Err(err) = &result {
                    collector.add_failure(TestFailure {
                        test_name: test_name.clone(),
                        error: format!("{err:#}"),
                        log_dir: Some(bitcoind_dir),
                    });
                }
                result
            })
        })
        .join();
        let result = match outcome {
            Ok(res) => {
                res.map_err(|e| libtest_mimic::Failed::from(format!("{e:#}")))
            }
            Err(_) => Err(libtest_mimic::Failed::from("test thread panicked")),
        };
        record_test_timing(timing_name, started.elapsed(), result.is_ok());
        result
    })
}

/// Whether `trial` would run under `args`, mirroring libtest_mimic's
/// name-based filtering (substring match, or exact with `--exact`, minus any
/// `--skip` patterns). Lets us detect an empty filter result before running.
fn filter_matches(args: &Arguments, trial: &Trial) -> bool {
    let name = trial.name();
    if let Some(filter) = &args.filter {
        let hit = if args.exact {
            name == filter
        } else {
            name.contains(filter)
        };
        if !hit {
            return false;
        }
    }
    for skip in &args.skip {
        let hit = if args.exact {
            name == skip
        } else {
            name.contains(skip)
        };
        if hit {
            return false;
        }
    }
    true
}

// MUST be run from within a tokio runtime
fn run() -> anyhow::Result<std::process::ExitCode> {
    let cli = Cli::parse();

    let rt_handle = tokio::runtime::Handle::current();

    set_tracing_subscriber(cli.log_level)?;

    let bin_paths = BinPaths::new();
    bin_paths.bitcoind().map_err(|err| {
        anyhow::anyhow!("{err}\n\nSet BITCOIND to a bitcoind binary.")
    })?;

    let collector = TestFailureCollector::new();

    type TrialFut = futures::future::BoxFuture<'static, anyhow::Result<()>>;
    type SetupFn = fn(TestSetup) -> TrialFut;
    type BareFn = fn(BinPaths, Directories) -> TrialFut;

    let setup_tests: &[(&str, SetupFn)] = &[
        ("block_connect_smoke", |s| {
            Box::pin(test_block_connect_smoke::test_block_connect_smoke(s))
        }),
        ("connect_block_deprioritizes_removed_txs", |s| {
            Box::pin(
                test_connect_block_deprioritizes_removed_txs::test_connect_block_deprioritizes_removed_txs(s),
            )
        }),
        ("disconnect_unrejects_removed_txs", |s| {
            Box::pin(
                test_disconnect_unrejects_removed_txs::test_disconnect_unrejects_removed_txs(s),
            )
        }),
        ("rejected_block_disconnect", |s| {
            Box::pin(
                test_rejected_block_disconnect::test_rejected_block_disconnect(
                    s,
                ),
            )
        }),
        ("reorg_re_inserts_tx", |s| {
            Box::pin(test_reorg_re_inserts_tx::test_reorg_re_inserts_tx(s))
        }),
        ("enforcer_rejection_during_reorg", |s| {
            Box::pin(
                test_enforcer_rejection_during_reorg::test_enforcer_rejection_during_reorg(s),
            )
        }),
        ("rbf_removed_for_absent_tx", |s| {
            Box::pin(
                test_rbf_removed_for_absent_tx::test_rbf_removed_for_absent_tx(
                    s,
                ),
            )
        }),
    ];

    let bare_tests: &[(&str, BareFn)] = &[
        ("accept_tx_paths", |bp, dirs| {
            Box::pin(test_accept_tx_paths::test_accept_tx_paths(bp, dirs))
        }),
        ("double_insert_after_reorg", |bp, dirs| {
            Box::pin(
                test_double_insert_after_reorg::test_double_insert_after_reorg(
                    bp, dirs,
                ),
            )
        }),
        ("disconnect_through_sync_tip", |bp, dirs| {
            Box::pin(
                test_disconnect_through_sync_tip::test_disconnect_through_sync_tip(
                    bp, dirs,
                ),
            )
        }),
        ("invalid_block_during_initial_sync", |bp, dirs| {
            Box::pin(
                test_invalid_block_during_initial_sync::test_invalid_block_during_initial_sync(
                    bp, dirs,
                ),
            )
        }),
        ("mempool_dat_fast_path", |bp, dirs| {
            Box::pin(test_mempool_dat_fast_path::test_mempool_dat_fast_path(
                bp, dirs,
            ))
        }),
        ("gbt_long_poll", |bp, dirs| {
            Box::pin(test_gbt_long_poll::test_gbt_long_poll(bp, dirs))
        }),
        // Same body, same assertions; only the shape of the in-flight
        // fetch differs. See the module docs.
        ("tx_replaced_during_fetch", |bp, dirs| {
            Box::pin(
                test_tx_replaced_during_fetch::test_tx_replaced_during_fetch(
                    bp,
                    dirs,
                    test_tx_replaced_during_fetch::FetchShape::Single,
                ),
            )
        }),
        ("tx_replaced_during_batch_fetch", |bp, dirs| {
            Box::pin(
                test_tx_replaced_during_fetch::test_tx_replaced_during_fetch(
                    bp,
                    dirs,
                    test_tx_replaced_during_fetch::FetchShape::Batched,
                ),
            )
        }),
    ];

    let mut trials = Vec::new();
    for (name, f) in setup_tests {
        let f = *f;
        trials.push(make_trial(
            name,
            bin_paths.clone(),
            collector.clone(),
            rt_handle.clone(),
            move |bp, dirs| async move {
                let setup = TestSetup::new(&bp, dirs).await?;
                f(setup).await
            },
        ));
    }
    for (name, f) in bare_tests {
        let f = *f;
        trials.push(make_trial(
            name,
            bin_paths.clone(),
            collector.clone(),
            rt_handle.clone(),
            f,
        ));
    }

    // Bail *before* running if a filter was provided but matches nothing —
    // otherwise libtest prints its "running 0 tests" banner and an "ok"
    // result line before we get a chance to error out.
    if cli.test_args.filter.is_some()
        && !trials
            .iter()
            .any(|trial| filter_matches(&cli.test_args, trial))
    {
        anyhow::bail!(
            "no integration test matched the provided filter `{}`",
            cli.test_args.filter.as_deref().unwrap_or("")
        );
    }

    let started = std::time::Instant::now();
    let exit_code = libtest_mimic::run(&cli.test_args, trials).exit_code();
    let wall = started.elapsed();

    // Per-test timing, then any failures at the end
    display_timing_summary(wall);
    collector.display_all_failures();
    Ok(exit_code)
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            std::process::ExitCode::from(1)
        }
    }
}
