#!/usr/bin/env bash
# Set up integration-test dependencies and write integrationtests.env.
# Idempotent. Re-running re-uses cached artifacts.
#
# The cusf-enforcer-mempool tests only need a stock Bitcoin Core (for bitcoind
# + bitcoin-cli) — no patched core, no electrs, no signet miner.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Deps live at the primary worktree so all worktrees of this clone share them.
GIT_COMMON_DIR="$(cd "$REPO_ROOT" && git rev-parse --git-common-dir)"
case "$GIT_COMMON_DIR" in
    /*) ;;
    *)  GIT_COMMON_DIR="$REPO_ROOT/$GIT_COMMON_DIR" ;;
esac
DEPS_ROOT="$(cd "$GIT_COMMON_DIR/.." && pwd)"
DEPS_DIR="$DEPS_ROOT/.integration-deps"

BITCOIN_VERSION="30.2"
STOCK_DIR="$DEPS_DIR/bitcoin-stock-$BITCOIN_VERSION"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$OS-$ARCH" in
    linux-x86_64)  STOCK_TARGET="x86_64-linux-gnu" ;;
    linux-aarch64) STOCK_TARGET="aarch64-linux-gnu" ;;
    darwin-x86_64) STOCK_TARGET="x86_64-apple-darwin" ;;
    darwin-arm64)  STOCK_TARGET="arm64-apple-darwin" ;;
    *) echo "Unsupported platform: $OS-$ARCH" >&2; exit 1 ;;
esac

mkdir -p "$DEPS_DIR"

# --- Stock Bitcoin Core ---
if [ ! -x "$STOCK_DIR/bitcoind" ]; then
    echo "Downloading stock Bitcoin Core $BITCOIN_VERSION ($STOCK_TARGET)..."
    TMP=$(mktemp -d)
    trap 'rm -rf "$TMP"' EXIT
    TARBALL="bitcoin-$BITCOIN_VERSION-$STOCK_TARGET.tar.gz"
    curl -# -fL "https://bitcoincore.org/bin/bitcoin-core-$BITCOIN_VERSION/$TARBALL" -o "$TMP/$TARBALL"
    tar -C "$TMP" -xf "$TMP/$TARBALL"
    rm -rf "$STOCK_DIR"
    mv "$TMP/bitcoin-$BITCOIN_VERSION/bin" "$STOCK_DIR"
    chmod +x "$STOCK_DIR"/bitcoind "$STOCK_DIR"/bitcoin-cli
    rm -rf "$TMP"
    trap - EXIT
else
    echo "Stock bitcoin: cached"
fi

# --- Write integrationtests.env ---
ENV_FILE="$REPO_ROOT/integrationtests.env"
cat > "$ENV_FILE" <<EOF
BITCOIND='$STOCK_DIR/bitcoind'
EOF

echo
echo "Wrote $ENV_FILE"
echo "Deps cache: $DEPS_DIR"
echo "Run integration tests with: just test-it"
