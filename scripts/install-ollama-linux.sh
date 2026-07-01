#!/usr/bin/env bash
set -euo pipefail

if command -v ollama >/dev/null 2>&1; then
  echo "Ollama is already installed: $(command -v ollama)"
  exit 0
fi

read -r -p "Install Ollama using the official installer from https://ollama.com/install.sh? [y/N] " answer
if [[ "${answer,,}" != "y" ]]; then
  echo "Skipped Ollama install."
  exit 0
fi

curl -fsSL https://ollama.com/install.sh | sh
