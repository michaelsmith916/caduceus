#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

./scripts/build.sh
platform="linux-$(go env GOARCH)"
install_dir="${HOME}/.local/bin"
mkdir -p "$install_dir"
cp "dist/${platform}/caduceusd" "dist/${platform}/caduceusctl" "dist/${platform}/caduceus-mcp" "$install_dir/"

"$install_dir/caduceusctl" init

if ! command -v ollama >/dev/null 2>&1; then
  read -r -p "Ollama was not found. Install Ollama now? [y/N] " answer
  if [[ "${answer,,}" == "y" ]]; then
    ./scripts/install-ollama-linux.sh
  fi
fi

if command -v systemctl >/dev/null 2>&1 && systemctl --user status >/dev/null 2>&1; then
  mkdir -p "${HOME}/.config/systemd/user"
  sed "s|@BIN_DIR@|${install_dir}|g" deploy/systemd/caduceusd.service > "${HOME}/.config/systemd/user/caduceusd.service"
  systemctl --user daemon-reload
  read -r -p "Enable and start caduceusd as a systemd user service? [y/N] " answer
  if [[ "${answer,,}" == "y" ]]; then
    systemctl --user enable --now caduceusd.service
  fi
else
  echo "systemd user services are unavailable. Start manually with: ${install_dir}/caduceusd"
fi

echo "Installed Caduceus binaries to ${install_dir}"
