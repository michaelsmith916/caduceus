#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

./scripts/build.sh
mkdir -p packages

for dir in dist/*; do
  [[ -d "$dir" ]] || continue
  name="$(basename "$dir")"
  if [[ "$name" == windows-* ]]; then
    (cd dist && zip -qr "../packages/caduceus-${name}.zip" "$name")
  else
    tar -C dist -czf "packages/caduceus-${name}.tar.gz" "$name"
  fi
done

echo "Packages written under packages/"
