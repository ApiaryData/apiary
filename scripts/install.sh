#!/usr/bin/env bash
# ============================================================
# Apiary install script
#
# Downloads a pre-built Apiary binary from GitHub Releases and
# installs it into /usr/local/bin (or a user-specified prefix).
#
# Supported platforms:
#   - Linux  x86_64  (amd64)
#   - Linux  aarch64 (arm64 / Raspberry Pi)
#   - macOS  x86_64
#   - macOS  aarch64 (Apple Silicon)
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/ApiaryData/apiary/main/scripts/install.sh | bash
#
#   # Or install a specific version / prefix:
#   APIARY_VERSION=0.1.0  INSTALL_DIR=$HOME/.local/bin  bash install.sh
# ============================================================

set -euo pipefail

REPO="ApiaryData/apiary"
VERSION="${APIARY_VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# ---- helpers ------------------------------------------------
info()  { printf "\033[1;34m==>\033[0m %s\n" "$*"; }
error() { printf "\033[1;31mERROR:\033[0m %s\n" "$*" >&2; exit 1; }

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || error "Required command not found: $1"
}

# ---- detect platform ----------------------------------------
detect_platform() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      error "Unsupported OS: $os" ;;
    esac

    case "$arch" in
        x86_64|amd64)          arch="x86_64" ;;
        aarch64|arm64)         arch="aarch64" ;;
        *)                     error "Unsupported architecture: $arch" ;;
    esac

    echo "${arch}-${os}"
}

# ---- resolve version ----------------------------------------
resolve_version() {
    if [ "$VERSION" = "latest" ]; then
        need_cmd curl
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' | head -1 | sed -E 's/.*"v?([^"]+)".*/\1/')
        [ -n "$VERSION" ] || error "Could not determine latest release version"
    fi
    # Strip leading "v" if present
    VERSION="${VERSION#v}"
    echo "$VERSION"
}

# ---- download & install ------------------------------------
main() {
    need_cmd curl
    need_cmd tar

    local platform
    platform="$(detect_platform)"
    VERSION="$(resolve_version)"

    local artifact="apiary-${platform}"
    local url="https://github.com/${REPO}/releases/download/v${VERSION}/${artifact}.tar.gz"

    info "Detected platform: ${platform}"
    info "Installing Apiary v${VERSION} → ${INSTALL_DIR}/apiary"

    local tmpdir
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    info "Downloading ${url} …"
    curl -fSL "$url" -o "${tmpdir}/${artifact}.tar.gz" \
        || error "Download failed. Check the version (v${VERSION}) and platform (${platform})."

    tar -xzf "${tmpdir}/${artifact}.tar.gz" -C "$tmpdir"

    # The archive contains a single binary named "apiary"
    if [ ! -f "${tmpdir}/apiary" ]; then
        error "Expected binary 'apiary' not found in archive"
    fi

    mkdir -p "$INSTALL_DIR"

    if [ -w "$INSTALL_DIR" ]; then
        mv "${tmpdir}/apiary" "${INSTALL_DIR}/apiary"
        chmod +x "${INSTALL_DIR}/apiary"
    else
        info "Installing to ${INSTALL_DIR} requires elevated privileges"
        sudo mv "${tmpdir}/apiary" "${INSTALL_DIR}/apiary"
        sudo chmod +x "${INSTALL_DIR}/apiary"
    fi

    info "Apiary v${VERSION} installed to ${INSTALL_DIR}/apiary"

    # Verify
    if command -v apiary >/dev/null 2>&1; then
        apiary --version 2>/dev/null || true
    else
        info "Add ${INSTALL_DIR} to your PATH if it is not already there."
    fi
}

main "$@"
