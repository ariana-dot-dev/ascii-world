#!/usr/bin/env sh
set -eu

INSTALL_DIR="${GAME_INSTALL_DIR:-}"
DOWNLOAD_BASE="${GAME_DOWNLOAD_BASE:-https://world.ascii.dev/download}"

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

ASSET="world-${OS}-${ARCH}"
URL="${DOWNLOAD_BASE}/${ASSET}"

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
  fi
fi

mkdir -p "$INSTALL_DIR"
TMP="$(mktemp)"
curl -fsSL "$URL" -o "$TMP"
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/world"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "Add $INSTALL_DIR to PATH to run world from any terminal." ;;
esac

echo "Installed world to $INSTALL_DIR/world"
echo "Run: world"
