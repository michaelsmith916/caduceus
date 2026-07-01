#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

./scripts/build.sh
platform="darwin-$(go env GOARCH)"
install_dir="${HOME}/.local/bin"
mkdir -p "$install_dir"
cp "dist/${platform}/caduceusd" "dist/${platform}/caduceusctl" "dist/${platform}/caduceus-mcp" "$install_dir/"

"$install_dir/caduceusctl" init

if ! command -v ollama >/dev/null 2>&1; then
  echo "Ollama was not found. Install from https://ollama.com or run: brew install ollama"
fi

read -r -p "Install launchd user agent? [y/N] " answer
if [[ "${answer,,}" == "y" ]]; then
  mkdir -p "${HOME}/Library/LaunchAgents"
  sed "s|@BIN_DIR@|${install_dir}|g" deploy/launchd/com.caduceus.caduceusd.plist > "${HOME}/Library/LaunchAgents/com.caduceus.caduceusd.plist"
  launchctl unload "${HOME}/Library/LaunchAgents/com.caduceus.caduceusd.plist" >/dev/null 2>&1 || true
  launchctl load "${HOME}/Library/LaunchAgents/com.caduceus.caduceusd.plist"
fi

echo "Installed Caduceus binaries to ${install_dir}"
