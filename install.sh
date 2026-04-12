#!/bin/sh

set -eu

REPO="${AGENT_COW_REPO:-h0ngcha0/agent-cow}"
VERSION="${AGENT_COW_VERSION:-latest}"
INSTALL_DIR="${AGENT_COW_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="agent-cow"
GITHUB_TOKEN="${AGENT_COW_GITHUB_TOKEN:-${GITHUB_TOKEN:-${GH_TOKEN:-}}}"

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required command not found: $1" >&2
        exit 1
    fi
}

download() {
    url="$1"
    out="$2"
    auth_header=""
    accept_header=""
    if [ -n "$GITHUB_TOKEN" ]; then
        auth_header="Authorization: Bearer $GITHUB_TOKEN"
        accept_header="Accept: application/octet-stream"
    fi
    if command -v curl >/dev/null 2>&1; then
        if [ -n "$GITHUB_TOKEN" ]; then
            curl -fsSL -H "$auth_header" ${accept_header:+-H "$accept_header"} "$url" -o "$out"
        else
            curl -fsSL "$url" -o "$out"
        fi
    elif command -v wget >/dev/null 2>&1; then
        if [ -n "$GITHUB_TOKEN" ]; then
            wget --header="$auth_header" --header="$accept_header" -qO "$out" "$url"
        else
            wget -qO "$out" "$url"
        fi
    else
        echo "error: either curl or wget is required" >&2
        exit 1
    fi
}

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "$uname_s" in
    Darwin)
        target_os="apple-darwin"
        ;;
    Linux)
        target_os="unknown-linux-gnu"
        ;;
    *)
        echo "error: unsupported operating system: $uname_s" >&2
        exit 1
        ;;
esac

case "$uname_m" in
    x86_64|amd64)
        target_arch="x86_64"
        ;;
    arm64|aarch64)
        target_arch="aarch64"
        ;;
    *)
        echo "error: unsupported architecture: $uname_m" >&2
        exit 1
        ;;
esac

if [ "$target_os" = "unknown-linux-gnu" ] && [ "$target_arch" != "x86_64" ]; then
    echo "error: published Linux binaries currently support x86_64 only" >&2
    exit 1
fi

need_cmd tar
need_cmd mktemp

archive="${BIN_NAME}-${target_arch}-${target_os}.tar.gz"

tmpdir="$(mktemp -d)"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

archive_path="$tmpdir/$archive"

if [ -n "$GITHUB_TOKEN" ]; then
    need_cmd python3
    if [ "$VERSION" = "latest" ]; then
        release_api="https://api.github.com/repos/${REPO}/releases/latest"
    else
        release_api="https://api.github.com/repos/${REPO}/releases/tags/${VERSION}"
    fi
    release_json="$tmpdir/release.json"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" "$release_api" -o "$release_json"
    else
        wget --header="Authorization: Bearer $GITHUB_TOKEN" -qO "$release_json" "$release_api"
    fi
    asset_api_url="$(python3 - "$release_json" "$archive" <<'PY'
import json, sys
path, wanted = sys.argv[1], sys.argv[2]
with open(path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)
for asset in data.get('assets', []):
    if asset.get('name') == wanted:
        print(asset.get('url', ''))
        break
PY
)"
    if [ -z "$asset_api_url" ]; then
        echo "error: release asset not found: $archive" >&2
        exit 1
    fi
    download "$asset_api_url" "$archive_path"
else
    if [ "$VERSION" = "latest" ]; then
        base_url="https://github.com/${REPO}/releases/latest/download"
    else
        base_url="https://github.com/${REPO}/releases/download/${VERSION}"
    fi
    download "$base_url/$archive" "$archive_path"
fi

tar -xzf "$archive_path" -C "$tmpdir"

mkdir -p "$INSTALL_DIR"
install_path="$INSTALL_DIR/$BIN_NAME"

if command -v install >/dev/null 2>&1; then
    install -m 0755 "$tmpdir/$BIN_NAME" "$install_path"
else
    cp "$tmpdir/$BIN_NAME" "$install_path"
    chmod 0755 "$install_path"
fi

echo "installed $BIN_NAME to $install_path"

case ":$PATH:" in
    *":$INSTALL_DIR:"*)
        ;;
    *)
        echo "note: $INSTALL_DIR is not in PATH" >&2
        echo "add this to your shell profile:" >&2
        echo "  export PATH=\"$INSTALL_DIR:\$PATH\"" >&2
        ;;
esac
