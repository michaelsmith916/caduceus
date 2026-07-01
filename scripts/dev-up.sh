#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

./scripts/build.sh
"$ROOT/dist/$(go env GOOS)-$(go env GOARCH)/caduceusctl" init
exec "$ROOT/dist/$(go env GOOS)-$(go env GOARCH)/caduceusd"
