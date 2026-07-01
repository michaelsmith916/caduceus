#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/caduceus-go-cache}"
export GOMODCACHE="${GOMODCACHE:-${TMPDIR:-/tmp}/caduceus-go-mod}"

CURRENT_GOOS="$(go env GOOS)"
CURRENT_GOARCH="$(go env GOARCH)"
TARGETS=(
  "linux/amd64"
  "linux/arm64"
  "darwin/amd64"
  "darwin/arm64"
  "windows/amd64"
)
BINARIES=(
  "caduceusd:./cmd/caduceusd"
  "caduceus-mcp:./cmd/caduceus-mcp"
  "caduceusctl:./cmd/caduceusctl"
  "caduceus-tray:./cmd/caduceus-tray"
)

mkdir -p dist

for target in "${TARGETS[@]}"; do
  goos="${target%/*}"
  goarch="${target#*/}"
  outdir="dist/${goos}-${goarch}"
  mkdir -p "$outdir"
  echo "==> building ${goos}/${goarch}"
  failed=0
  for entry in "${BINARIES[@]}"; do
    name="${entry%%:*}"
    pkg="${entry#*:}"
    exe="$name"
    if [[ "$goos" == "windows" ]]; then
      exe="${name}.exe"
    fi
    if ! CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -ldflags="-s -w" -o "${outdir}/${exe}" "$pkg"; then
      failed=1
      break
    fi
  done
  if [[ "$failed" -ne 0 ]]; then
    if [[ "$goos" == "$CURRENT_GOOS" && "$goarch" == "$CURRENT_GOARCH" ]]; then
      echo "current platform build failed: ${goos}/${goarch}" >&2
      exit 1
    fi
    echo "warning: best-effort cross build failed for ${goos}/${goarch}" >&2
  fi
done

echo "Binaries written under dist/"
