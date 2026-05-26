use std::time::Duration;

use clap::Parser;
use cusf_enforcer_mempool_integration_tests::{
    setup::{Directories, TestSetup},
    test_accept_tx_paths, test_block_connect_smoke,
    test_compose_drops_remove_mempool_txs, test_double_insert_after_reorg,
    test_enforcer_rejection_during_reorg, test_rbf_removed_for_absent_tx,
    test_reorg_re_inserts_tx,
    util::{BinPaths, TestFailure, TestFailureCollector},
};
use libtest_mimic::{Arguments, Trial};
use tokio_util::task::AbortOnDropHandle;
use tracing_subscriber::{
    filter as tracing_filter, layer::SubscriberExt, util::SubscriberInitExt,
};

const PER_TEST_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Parser)]
struct Cli {
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

fn set_tracing_subscriber(log_level: tracing::Level) -> anyhow::Result<()> {
    let targets_filter = {
        let defaults = targets_directive_str([
            ("", saturating_pred_level(log_level)),
            ("cusf_enforcer_mempool", log_level),
            ("cusf_enforcer_mempool_integration_tests", log_level),
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
                .with_target(false)
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
        let span_name = span_name.clone();
        let outcome = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            rt.block_on(async move {
                let span = tracing::info_span!("test", name = %span_name);
                let _entered = span.enter();
                let dirs = Directories::new()?;
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
        .join()
        .map_err(|_| libtest_mimic::Failed::from("test thread panicked"))?;
        outcome.map_err(|e| libtest_mimic::Failed::from(format!("{e:#}")))
    })
}

fn run() -> anyhow::Result<std::process::ExitCode> {
    let cli = Cli::parse();
    set_tracing_subscriber(tracing::Level::DEBUG)?;

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
        ("compose_drops_remove_mempool_txs", |bp, dirs| {
            Box::pin(
                test_compose_drops_remove_mempool_txs::test_compose_drops_remove_mempool_txs(bp, dirs),
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
            move |bp, dirs| async move {
                let setup = TestSetup::new(&bp, dirs).await?;
                f(setup).await
            },
        ));
    }
    for (name, f) in bare_tests {
        let f = *f;
        trials.push(make_trial(name, bin_paths.clone(), collector.clone(), f));
    }

    let exit_code = libtest_mimic::run(&cli.test_args, trials).exit_code();
    collector.display_all_failures();
    Ok(exit_code)
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            std::process::ExitCode::from(1)
        }
    }
}
