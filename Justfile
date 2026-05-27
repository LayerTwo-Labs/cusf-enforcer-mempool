build *args='':
    cargo build {{ args }}

# Run linter
clippy:
    cargo clippy --all-targets -- --deny warnings

# Format code
fmt:
    cargo fmt --all

# Run the integration tests. Forwards extra args to libtest-mimic.
# Examples:
#   just test-it
#   just test-it basic_sync
#   just test-it --nocapture reorg_re_inserts_tx
[doc('Run integration tests')]
test-it *args='': 
    #!/usr/bin/env bash
    set -euo pipefail
    set -a
    source ./integrationtests.env
    set +a
    cargo run --bin integration_tests -- {{ args }}
