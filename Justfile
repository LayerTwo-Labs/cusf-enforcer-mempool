build *args='':
    cargo build {{ args }}

# Run linter
clippy:
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged -- --deny warnings

# Format code
fmt:
    cargo fmt --all

# Run the integration tests. Forwards extra args to libtest-mimic.
# Harness logs are off by default; turn them on with --log-level.
# Examples:
#   just test-it
#   just test-it block_connect_smoke
#   just test-it --log-level debug reorg_re_inserts_tx
[doc('Run integration tests')]
test-it *args='': 
    #!/usr/bin/env bash
    set -euo pipefail
    set -a
    source ./integrationtests.env
    set +a
    cargo run --bin integration_tests -- {{ args }}
