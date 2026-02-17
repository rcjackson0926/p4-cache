#!/usr/bin/env bash
#
# Build a .deb package for p4-cache.
#
# Usage:
#   ./package/build-deb.sh              # uses binaries from build/
#   ./package/build-deb.sh --rebuild    # runs cmake+make first
#
# Output: p4-cache_<version>_<arch>.deb in the current directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"

# Read version from CMakeLists.txt
VERSION=$(grep -oP 'project\(p4-cache VERSION \K[0-9]+\.[0-9]+\.[0-9]+' "$REPO_ROOT/CMakeLists.txt")
if [[ -z "$VERSION" ]]; then
    echo "Error: could not parse version from CMakeLists.txt" >&2
    exit 1
fi

ARCH=$(dpkg --print-architecture)
PKG_NAME="p4-cache"
PKG_DIR="$(mktemp -d)"
trap 'rm -rf "$PKG_DIR"' EXIT

echo "Building ${PKG_NAME}_${VERSION}_${ARCH}.deb ..."

# --- Optional rebuild ---
if [[ "${1:-}" == "--rebuild" ]]; then
    echo "==> Rebuilding from source ..."
    mkdir -p "$BUILD_DIR"
    (cd "$BUILD_DIR" && cmake .. && make -j"$(nproc)")
fi

# --- Verify binaries exist ---
for bin in "$BUILD_DIR/p4-cache" "$BUILD_DIR/libp4shim.so"; do
    if [[ ! -f "$bin" ]]; then
        echo "Error: $bin not found. Run with --rebuild or build first." >&2
        exit 1
    fi
done

# --- Populate package tree ---

# Binaries
install -Dm755 "$BUILD_DIR/p4-cache"     "$PKG_DIR/usr/local/bin/p4-cache"
install -Dm755 "$BUILD_DIR/libp4shim.so"  "$PKG_DIR/usr/local/lib/libp4shim.so"

# systemd service
install -Dm644 /dev/stdin "$PKG_DIR/lib/systemd/system/p4-cache.service" <<'SERVICE'
[Unit]
Description=P4 Cache - NVMe depot acceleration daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=perforce
Group=perforce

ExecStart=/usr/local/bin/p4-cache \
    --config /etc/p4-cache/config.json \
    --daemon \
    --pid-file /run/p4-cache.pid \
    --log-file /var/log/p4-cache.log
PIDFile=/run/p4-cache.pid

Restart=on-failure
RestartSec=5

AmbientCapabilities=CAP_SYS_ADMIN
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SERVICE

# Example config (goes to /etc, marked as conffile below)
install -Dm640 /dev/stdin "$PKG_DIR/etc/p4-cache/config.json" <<'CONFIG'
{
  "depot_path": "/mnt/nvme/depot",
  "primary": {
    "type": "s3",
    "bucket": "CHANGEME",
    "region": "us-east-1"
  },
  "max_cache_gb": 500,
  "low_watermark_gb": 400,
  "upload_threads": 8,
  "upload_concurrency": 16,
  "restore_threads": 16,
  "stats_interval": 60
}
CONFIG

# --- DEBIAN control files ---

install -dm755 "$PKG_DIR/DEBIAN"

# Installed size in KB
INSTALLED_KB=$(du -sk "$PKG_DIR" --exclude=DEBIAN | cut -f1)

cat > "$PKG_DIR/DEBIAN/control" <<EOF
Package: ${PKG_NAME}
Version: ${VERSION}
Architecture: ${ARCH}
Maintainer: RCJackson Consulting <rusty@rcjacksonconsulting.com>
Depends: libsqlite3-0, libcurl4t64 | libcurl4, libssl3t64 | libssl3, zlib1g
Recommends: socat
Section: admin
Priority: optional
Installed-Size: ${INSTALLED_KB}
Homepage: https://www.perforce.com
Description: NVMe depot acceleration daemon for Perforce
 p4-cache provides a transparent NVMe caching layer between Perforce (P4d)
 and remote object storage (S3, Azure Blob, GCS, or NFS). It uploads new
 depot files in the background, evicts cold files to 0-byte stubs, and
 restores them on demand via an LD_PRELOAD shim.
 .
 Package contents:
  - p4-cache: the cache daemon
  - libp4shim.so: LD_PRELOAD library for P4d cold-file interception
EOF

# Conffiles: dpkg won't overwrite user edits on upgrade
cat > "$PKG_DIR/DEBIAN/conffiles" <<'EOF'
/etc/p4-cache/config.json
EOF

# postinst: setcap + ldconfig + hint
cat > "$PKG_DIR/DEBIAN/postinst" <<'POSTINST'
#!/bin/sh
set -e

# Make the shim discoverable by the dynamic linker
ldconfig

# Grant fanotify capability (needed for read-write mode, not read-only)
if command -v setcap >/dev/null 2>&1; then
    setcap cap_sys_admin+ep /usr/local/bin/p4-cache || true
fi

# Reload systemd if present
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi

echo ""
echo "p4-cache installed."
echo ""
echo "  1. Edit /etc/p4-cache/config.json with your depot path and storage backend."
echo "  2. Enable the service:  systemctl enable --now p4-cache"
echo "  3. Start P4d with the shim:"
echo "       LD_PRELOAD=/usr/local/lib/libp4shim.so P4CACHE_DEPOT=/mnt/nvme/depot p4d ..."
echo ""
POSTINST
chmod 755 "$PKG_DIR/DEBIAN/postinst"

# postrm: cleanup on purge
cat > "$PKG_DIR/DEBIAN/postrm" <<'POSTRM'
#!/bin/sh
set -e

ldconfig

if [ "$1" = "purge" ]; then
    rm -rf /etc/p4-cache
fi

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi
POSTRM
chmod 755 "$PKG_DIR/DEBIAN/postrm"

# prerm: stop service before removal
cat > "$PKG_DIR/DEBIAN/prerm" <<'PRERM'
#!/bin/sh
set -e

if [ "$1" = "remove" ] || [ "$1" = "deconfigure" ]; then
    if [ -d /run/systemd/system ]; then
        systemctl stop p4-cache 2>/dev/null || true
        systemctl disable p4-cache 2>/dev/null || true
    fi
fi
PRERM
chmod 755 "$PKG_DIR/DEBIAN/prerm"

# --- Build the .deb ---

DEB_FILE="${PKG_NAME}_${VERSION}_${ARCH}.deb"
dpkg-deb --root-owner-group --build "$PKG_DIR" "$REPO_ROOT/$DEB_FILE"

echo ""
echo "Package built: $REPO_ROOT/$DEB_FILE"
echo ""
echo "Install:   sudo dpkg -i $DEB_FILE"
echo "Verify:    dpkg -c $DEB_FILE"
