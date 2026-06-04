#!/usr/bin/env sh
set -eu

REPO="${GAME_CLI_REPO:-REPLACE_WITH_GITHUB_REPO}"
INSTALL_DIR="${GAME_INSTALL_DIR:-$HOME/.ascii/bin}"

case "$(uname -s)" in
  Linux) OS="linux" ;;
  Darwin) OS="darwin" ;;
  *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64) ARCH="x64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

ASSET="game-${OS}-${ARCH}"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"

mkdir -p "$INSTALL_DIR"
TMP="$(mktemp)"
curl -fsSL "$URL" -o "$TMP"
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/game"

echo "Installed game to $INSTALL_DIR/game"

