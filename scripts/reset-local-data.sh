#!/usr/bin/env sh
set -eu

if [ -n "${XDG_DATA_HOME:-}" ]; then
  DATA_DIR="$XDG_DATA_HOME"
elif [ "$(uname -s)" = "Darwin" ]; then
  DATA_DIR="$HOME/Library/Application Support"
else
  DATA_DIR="$HOME/.local/share"
fi

TARGET="$DATA_DIR/ascii-game"

if [ ! -e "$TARGET" ]; then
  echo "No local data found at $TARGET"
  exit 0
fi

rm -rf "$TARGET"
echo "Removed $TARGET"
