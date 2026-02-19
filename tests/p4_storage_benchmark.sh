#!/bin/bash
# ============================================================
# P4 Storage Backend Performance Benchmark
# ============================================================
#
# Self-contained benchmark that starts a local p4d server,
# generates test data, creates separate depots for each storage
# backend, runs performance tests, and cleans everything up on
# exit.
#
# Each backend gets its own depot.  The local and NFS backends use
# native Perforce depot Map settings.  The S3 backend uses p4-cache
# (NVMe caching daemon) with a local depot -- p4d writes to local
# disk at full speed while p4-cache uploads to S3 in the background.
#
# PREREQUISITES
# -------------
#   - p4 and p4d binaries  (set P4 / P4D if not in default location)
#   - Standard coreutils:  bc, numfmt, dd
#   - For NFS backend:     a mounted NFS filesystem (set NFS_MOUNT)
#   - For S3 backend:      a running S3/MinIO endpoint;
#                           p4-cache and libp4shim.so binaries (built from this repo)
#
# QUICK START
# -----------
#   # Local-only test -- fully self-contained, nothing else needed
#   ./p4_storage_benchmark.sh
#
#   # Local vs NFS
#   BACKENDS=local,nfs NFS_MOUNT=/mnt/nfs ./p4_storage_benchmark.sh
#
#   # Local vs S3 (MinIO)
#   BACKENDS=local,s3 \
#     S3_ENDPOINT=https://minio:9000 S3_BUCKET=p4-bench \
#     S3_ACCESS_KEY=minio-user S3_SECRET_KEY=secret \
#     ./p4_storage_benchmark.sh
#
#   # All three backends
#   BACKENDS=local,nfs,s3 NFS_MOUNT=/mnt/nfs \
#     S3_ENDPOINT=https://minio:9000 S3_BUCKET=p4-bench \
#     ./p4_storage_benchmark.sh
#
#   # Custom p4/p4d location
#   P4=/opt/perforce/bin/p4 P4D=/opt/perforce/sbin/p4d \
#     ./p4_storage_benchmark.sh
#
# HOW IT WORKS
# ------------
#   1. Generates random test files (1KB through 100MB)
#   2. Starts a temporary local p4d server
#   3. Creates one depot per backend:
#        local  Standard depot, files under p4d server root.
#        nfs    Depot Map is an absolute path on the NFS mount,
#               so p4d reads/writes over NFS directly.
#        s3     Local depot + p4-cache daemon.  p4d writes to local disk;
#               p4-cache uploads to S3 in the background.  Reads of evicted
#               files are intercepted by the LD_PRELOAD shim (libp4shim.so).
#   4. Creates a user and one workspace per depot
#   5. Runs add/submit, edit/submit, and sync benchmarks
#   6. Cleans up on exit (p4d, p4-cache, depots, test data, workspaces, S3 objects)
#
#   Results CSV is preserved after cleanup.
#
# ENVIRONMENT VARIABLES
# ---------------------
#   Variable        Default                  Description
#   --------        -------                  -----------
#   P4              /p4/common/bin/p4         p4 CLI binary
#   P4D             /p4/common/bin/p4d        p4d server binary
#   P4PORT          16700                     Port for local p4d
#   P4USER          benchuser                 P4 user for benchmarks
#   BACKENDS        local                     Comma-separated: local, s3, nfs
#   TESTDIR         /tmp/p4-bench             Working directory
#   S3_ENDPOINT     https://localhost:9000     S3/MinIO endpoint URL
#   S3_BUCKET       p4-test-depot             S3 bucket name
#   S3_ACCESS_KEY   minio-user                S3 access key
#   S3_SECRET_KEY   P@ssw0rd                  S3 secret key
#   S3_REGION       us-east-1                 S3 region
#   NFS_MOUNT       /mnt/perforce             NFS mount point
#   NFS_DEPOT_ROOT  $NFS_MOUNT/test/p4-bench  Writable dir on NFS for depot data
#   DEPOT_LOCAL     bench-local               Local-backed depot name
#   DEPOT_S3        bench-s3                  S3-backed depot name
#   DEPOT_NFS       bench-nfs                 NFS-backed depot name
#   P4CACHE_BIN     <repo>/build/p4-cache     p4-cache daemon binary
#   P4SHIM_LIB      <repo>/build/libp4shim.so LD_PRELOAD shim library
#
# LAB ENVIRONMENT (u431 / ct-host-u431)
# --------------------------------------
#   # Clear production P4 env vars first, then run all three backends:
#   env -u P4CONFIG -u P4TICKETS -u P4TRUST -u P4PORT -u P4USER -u P4CLIENT \
#     BACKENDS=local,nfs,s3 \
#     NFS_MOUNT=/mnt/perforce \
#     S3_ENDPOINT=https://localhost:9000 \
#     S3_BUCKET=p4-test-depot \
#     S3_ACCESS_KEY=minio-user \
#     S3_SECRET_KEY='P@ssw0rd' \
#     TESTDIR=/mnt/p4meta/test/bench \
#     bash p4_storage_benchmark.sh
#
#   MinIO runs on localhost:9000 (HTTPS). p4d/p4 from /p4/common/bin/.
#   NFS writable path: /mnt/perforce/test/p4-bench (NFS root is owned by root).
#   Results saved to: meridian/tests/p4_storage_benchmark_results.csv
#
# OUTPUT
# ------
#   Console:  human-readable per-operation timings
#   CSV:      $TESTDIR/results.csv
#             columns: category,backend,operation,files,total_bytes,elapsed_sec
# ============================================================
set -e

# --- Configuration ------------------------------------------------
P4=${P4:-/p4/common/bin/p4}
P4D_BIN=${P4D:-/p4/common/bin/p4d}
P4PORT=${P4PORT:-16700}
P4USER=${P4USER:-benchuser}
BACKENDS=${BACKENDS:-local}
TESTDIR=${TESTDIR:-/tmp/p4-bench}
S3_ENDPOINT=${S3_ENDPOINT:-https://localhost:9000}
S3_BUCKET=${S3_BUCKET:-p4-test-depot}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-minio-user}
S3_SECRET_KEY=${S3_SECRET_KEY:-P@ssw0rd}
S3_REGION=${S3_REGION:-us-east-1}
NFS_MOUNT=${NFS_MOUNT:-/mnt/perforce}
NFS_DEPOT_ROOT=${NFS_DEPOT_ROOT:-${NFS_MOUNT}/test/p4-bench}
DEPOT_LOCAL=${DEPOT_LOCAL:-bench-local}
DEPOT_S3=${DEPOT_S3:-bench-s3}
DEPOT_NFS=${DEPOT_NFS:-bench-nfs}

# p4-cache binaries (S3 backend uses p4-cache instead of p4d native S3 support)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
P4CACHE_BIN=${P4CACHE_BIN:-${REPO_ROOT}/build/p4-cache}
P4SHIM_LIB=${P4SHIM_LIB:-${REPO_ROOT}/build/libp4shim.so}
P4CACHE_PID=""

mkdir -p "$TESTDIR"
RESULTS_FILE=$TESTDIR/results.csv

P4BASE="$P4 -p $P4PORT -u $P4USER"
P4ROOT=""
P4D_PID=""

# Parse and validate BACKENDS
IFS=',' read -ra BACKEND_LIST <<< "$BACKENDS"
for b in "${BACKEND_LIST[@]}"; do
    case "$b" in
        local|s3|nfs) ;;
        *) echo "ERROR: Unknown backend '$b'. Valid: local, s3, nfs" >&2; exit 1 ;;
    esac
done

# Check that p4 and p4d binaries exist
for bin in "$P4" "$P4D_BIN"; do
    if ! command -v "$bin" >/dev/null 2>&1 && [ ! -x "$bin" ]; then
        echo "ERROR: '$bin' not found or not executable." >&2
        echo "Set P4 and P4D environment variables to the correct paths." >&2
        exit 1
    fi
done

# Check p4-cache binaries when S3 backend is selected
for b in "${BACKEND_LIST[@]}"; do
    if [ "$b" = "s3" ]; then
        for bin in "$P4CACHE_BIN" "$P4SHIM_LIB"; do
            if [ ! -f "$bin" ]; then
                echo "ERROR: '$bin' not found." >&2
                echo "Build p4-cache first: cd build && cmake .. && make -j8" >&2
                echo "Or set P4CACHE_BIN / P4SHIM_LIB environment variables." >&2
                exit 1
            fi
        done
        break
    fi
done

# Check for curl with --aws-sigv4 (curl 7.75+, needed for S3 cleanup)
for b in "${BACKEND_LIST[@]}"; do
    if [ "$b" = "s3" ]; then
        if ! curl --aws-sigv4 "test" 2>&1 | grep -q "option --aws-sigv4" 2>/dev/null; then
            # curl either supports it or doesn't know the option
            if ! curl --help all 2>&1 | grep -q aws-sigv4; then
                echo "WARNING: curl lacks --aws-sigv4 (need 7.75+). S3 cleanup will not work." >&2
                echo "         Install a newer curl, or clean up S3 objects manually." >&2
            fi
        fi
        break
    fi
done

# --- Helpers ------------------------------------------------------
depot_for() {
    case "$1" in
        local) echo "$DEPOT_LOCAL" ;;
        s3)    echo "$DEPOT_S3" ;;
        nfs)   echo "$DEPOT_NFS" ;;
    esac
}

# --- S3 helpers (curl --aws-sigv4, no boto3/aws CLI needed) -------

# Call S3 API via curl with native AWS Signature V4 (curl 7.75+).
# Usage: s3_curl <METHOD> <url_path_and_query> [extra_curl_args...]
# The URL is relative to the bucket: s3_curl GET "/?list-type=2"
s3_curl() {
    local method="$1" path_query="$2"
    shift 2

    local curl_insecure=""
    case "$S3_ENDPOINT" in https://*) curl_insecure="-k" ;; esac

    curl -sf $curl_insecure \
        -X "$method" \
        --aws-sigv4 "aws:amz:${S3_REGION}:s3" \
        -u "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" \
        "$@" \
        "${S3_ENDPOINT}/${S3_BUCKET}${path_query}"
}

# Delete all S3 objects under a given prefix.
# Usage: s3_delete_prefix <prefix>
s3_delete_prefix() {
    local prefix="$1"
    local deleted=0
    local continuation=""

    while true; do
        local list_args="?list-type=2&prefix=${prefix}&max-keys=1000"
        [ -n "$continuation" ] && list_args="${list_args}&continuation-token=${continuation}"

        local response
        response=$(s3_curl "GET" "/${list_args}" 2>/dev/null) || {
            echo "  S3 list failed. Remove objects manually: s3://$S3_BUCKET/$prefix" >&2
            return 0
        }

        # Parse keys from XML response
        local keys
        keys=$(echo "$response" | grep -oP '<Key>\K[^<]+' 2>/dev/null || true)
        [ -z "$keys" ] && break

        while IFS= read -r key; do
            # URL-encode characters that break AWS SigV4 signing (commas, spaces, etc.)
            local encoded_key
            encoded_key=$(echo -n "$key" | sed 's/,/%2C/g; s/ /%20/g; s/+/%2B/g')
            s3_curl "DELETE" "/${encoded_key}" 2>/dev/null && deleted=$((deleted + 1)) || true
        done <<< "$keys"

        # Check for truncation / pagination
        if echo "$response" | grep -q '<IsTruncated>true</IsTruncated>' 2>/dev/null; then
            continuation=$(echo "$response" | grep -oP '<NextContinuationToken>\K[^<]+' 2>/dev/null || true)
            [ -z "$continuation" ] && break
        else
            break
        fi
    done

    if [ "$deleted" -gt 0 ]; then
        echo "  Removed $deleted S3 objects."
    else
        echo "  No S3 objects to remove."
    fi
}

# --- Cleanup helpers ----------------------------------------------

# Remove leftover test directories, NFS depot data, and S3 objects.
# Called both at startup (to clear stale data from previous runs) and
# at exit via the trap.
clean_test_artifacts() {
    # Remove any old p4root dirs left by previous runs
    for d in "$TESTDIR"/p4root.*; do
        [ -d "$d" ] && rm -rf "$d" && echo "  Removed stale p4root: $d"
    done

    # Remove generated test data
    if [ -d "$TESTDIR/testdata" ]; then
        rm -rf "$TESTDIR/testdata"
        echo "  Removed test data."
    fi

    # Remove workspace directories
    for backend in "${BACKEND_LIST[@]}"; do
        if [ -d "$TESTDIR/ws-${backend}" ]; then
            rm -rf "$TESTDIR/ws-${backend}"
            echo "  Removed workspace: ws-${backend}"
        fi
    done

    # Remove NFS depot data
    for backend in "${BACKEND_LIST[@]}"; do
        if [ "$backend" = "nfs" ]; then
            local nfs_path="${NFS_DEPOT_ROOT}/$(depot_for nfs)"
            if [ -d "$nfs_path" ]; then
                rm -rf "$nfs_path"
                rmdir "$NFS_DEPOT_ROOT" 2>/dev/null || true
                echo "  Removed NFS depot data: $nfs_path"
            fi
        fi
    done

    # Remove S3 depot objects
    for backend in "${BACKEND_LIST[@]}"; do
        if [ "$backend" = "s3" ]; then
            local s3_depot
            s3_depot=$(depot_for s3)
            echo "  Cleaning S3 objects (bucket: $S3_BUCKET, prefix: $s3_depot/) ..."
            s3_delete_prefix "$s3_depot/"
        fi
    done
}

# Kill any p4d still listening on our benchmark port from a previous run.
kill_stale_p4d() {
    local stale_pid
    stale_pid=$(fuser "${P4PORT}/tcp" 2>/dev/null | tr -d ' ') || true
    if [ -n "$stale_pid" ]; then
        echo "  Killing stale process on port $P4PORT (PID $stale_pid) ..."
        kill "$stale_pid" 2>/dev/null || true
        sleep 1
    fi
}

# --- Cleanup (runs on EXIT, INT, TERM) ----------------------------
cleanup() {
    local rc=$?
    echo ""
    echo "Cleaning up ..."

    # Stop p4d
    if [ -n "$P4D_PID" ]; then
        kill "$P4D_PID" 2>/dev/null || true
        wait "$P4D_PID" 2>/dev/null || true
        echo "  Stopped p4d (PID $P4D_PID)."
    fi

    # Stop p4-cache daemon
    if [ -n "$P4CACHE_PID" ]; then
        kill "$P4CACHE_PID" 2>/dev/null || true
        wait "$P4CACHE_PID" 2>/dev/null || true
        echo "  Stopped p4-cache (PID $P4CACHE_PID)."
    fi

    # Preserve logs for debugging, then remove server root
    if [ -n "$P4ROOT" ] && [ -d "$P4ROOT" ]; then
        if [ -f "$P4ROOT/p4d.log" ]; then
            cp "$P4ROOT/p4d.log" "$TESTDIR/p4d.log"
            echo "  Saved p4d log to $TESTDIR/p4d.log"
        fi
        if [ -f "$P4ROOT/p4cache.log" ]; then
            cp "$P4ROOT/p4cache.log" "$TESTDIR/p4cache.log"
            echo "  Saved p4-cache log to $TESTDIR/p4cache.log"
        fi
        rm -rf "$P4ROOT"
        echo "  Removed p4d root: $P4ROOT"
    fi

    clean_test_artifacts

    echo "Cleanup complete.  Results preserved at $RESULTS_FILE"
    exit $rc
}
trap cleanup EXIT INT TERM

# --- Test data generation -----------------------------------------
generate_testdata() {
    local dir=$TESTDIR/testdata

    echo "Generating test data in $dir ..."
    mkdir -p "$dir/small" "$dir/medium" "$dir/large" "$dir/xlarge" "$dir/huge"

    echo "  small:  100 x 1KB ..."
    for i in $(seq 1 100); do
        dd if=/dev/urandom of="$dir/small/file_${i}.bin" bs=1024 count=1 2>/dev/null
    done

    echo "  medium: 50 x 100KB ..."
    for i in $(seq 1 50); do
        dd if=/dev/urandom of="$dir/medium/file_${i}.bin" bs=102400 count=1 2>/dev/null
    done

    echo "  large:  20 x 1MB ..."
    for i in $(seq 1 20); do
        dd if=/dev/urandom of="$dir/large/file_${i}.bin" bs=1048576 count=1 2>/dev/null
    done

    echo "  xlarge: 5 x 10MB ..."
    for i in $(seq 1 5); do
        dd if=/dev/urandom of="$dir/xlarge/file_${i}.bin" bs=10485760 count=1 2>/dev/null
    done

    echo "  huge:   5 x 100MB ..."
    for i in $(seq 1 5); do
        dd if=/dev/urandom of="$dir/huge/file_${i}.bin" bs=1048576 count=100 2>/dev/null
    done

    echo "Test data generated."
}

# --- S3 SSL certificate setup ------------------------------------
# p4-cache uses --primary-no-verify-ssl to skip certificate validation,
# so this setup is only needed for the s3_curl cleanup helper and for
# any direct curl verification calls.
#
# Strategy:
#   1. Extract the cert from the endpoint
#   2. Set CA env vars (works with some libcurl builds)
#   3. Install the cert system-wide via update-ca-certificates (reliable)
#   4. If all else fails, suggest HTTP fallback
setup_s3_ssl() {
    # Only needed for HTTPS endpoints when S3 backend is enabled
    case "$S3_ENDPOINT" in
        https://*) ;;
        *) return 0 ;;
    esac

    local has_s3=false
    for b in "${BACKEND_LIST[@]}"; do
        [ "$b" = "s3" ] && has_s3=true
    done
    $has_s3 || return 0

    # Extract host:port from endpoint URL
    local s3_hostport
    s3_hostport=$(echo "$S3_ENDPOINT" | sed 's|^https://||; s|/.*||')
    case "$s3_hostport" in
        *:*) ;;
        *)   s3_hostport="${s3_hostport}:443" ;;
    esac

    # Check if the system already trusts this cert
    if echo | openssl s_client -connect "$s3_hostport" 2>/dev/null | grep -q "Verify return code: 0"; then
        echo "S3 endpoint SSL certificate is already trusted by system."
        return 0
    fi

    echo "S3 endpoint uses untrusted certificate — extracting ..."
    local ca_file="$TESTDIR/s3-ca.crt"

    if ! echo | openssl s_client -showcerts -connect "$s3_hostport" 2>/dev/null \
         | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/' > "$ca_file" 2>/dev/null \
       || [ ! -s "$ca_file" ]; then
        echo "  WARNING: Could not extract certificate from $s3_hostport" >&2
        echo "  Try S3_ENDPOINT=http://... as fallback." >&2
        return 0
    fi

    # --- Attempt 1: env vars (works with some libcurl builds) ---
    local sys_ca=""
    for f in /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt \
             /etc/ssl/cert.pem /usr/local/share/certs/ca-root-nss.crt; do
        [ -f "$f" ] && sys_ca="$f" && break
    done

    local ca_bundle="$TESTDIR/s3-ca-bundle.crt"
    cp "$ca_file" "$ca_bundle"
    [ -n "$sys_ca" ] && cat "$sys_ca" >> "$ca_bundle"

    export CURL_CA_BUNDLE="$ca_bundle"
    export SSL_CERT_FILE="$ca_bundle"
    export REQUESTS_CA_BUNDLE="$ca_bundle"
    export SSL_CERT_DIR="$(dirname "$ca_bundle")"

    # --- Attempt 2: install system-wide (most reliable) ---
    # Some p4d builds statically link libcurl and ignore env vars.
    # Installing the cert system-wide always works.
    local installed_system=false
    if [ -d /usr/local/share/ca-certificates ]; then
        # Debian/Ubuntu path
        if ! cmp -s "$ca_file" /usr/local/share/ca-certificates/s3-benchmark.crt 2>/dev/null; then
            echo "  Installing certificate system-wide (may need sudo) ..."
            if sudo -n cp "$ca_file" /usr/local/share/ca-certificates/s3-benchmark.crt 2>/dev/null \
               && sudo -n update-ca-certificates 2>/dev/null; then
                installed_system=true
                echo "  Installed to /usr/local/share/ca-certificates/s3-benchmark.crt"
            else
                echo "  Could not install system-wide without password."
            fi
        else
            installed_system=true
            echo "  Certificate already installed system-wide."
        fi
    elif [ -d /etc/pki/ca-trust/source/anchors ]; then
        # RHEL/CentOS path
        if ! cmp -s "$ca_file" /etc/pki/ca-trust/source/anchors/s3-benchmark.crt 2>/dev/null; then
            echo "  Installing certificate system-wide (may need sudo) ..."
            if sudo -n cp "$ca_file" /etc/pki/ca-trust/source/anchors/s3-benchmark.crt 2>/dev/null \
               && sudo -n update-ca-trust 2>/dev/null; then
                installed_system=true
                echo "  Installed to /etc/pki/ca-trust/source/anchors/s3-benchmark.crt"
            else
                echo "  Could not install system-wide without password."
            fi
        else
            installed_system=true
            echo "  Certificate already installed system-wide."
        fi
    fi

    # --- Verify ---
    local s3_host
    s3_host=$(echo "$S3_ENDPOINT" | sed 's|^https://||; s|/.*||')
    if command -v curl >/dev/null 2>&1; then
        if curl -sf -o /dev/null "$S3_ENDPOINT" 2>/dev/null; then
            echo "  Verified: curl can reach $S3_ENDPOINT."
        elif curl --cacert "$ca_bundle" -sf -o /dev/null "$S3_ENDPOINT" 2>/dev/null; then
            echo "  Verified: curl can reach $S3_ENDPOINT with extracted cert."
        elif ! $installed_system; then
            echo "  WARNING: p4d will likely fail to reach S3." >&2
            echo "  Fix: install cert system-wide, or use HTTP:" >&2
            echo "    sudo cp $ca_file /usr/local/share/ca-certificates/s3-benchmark.crt" >&2
            echo "    sudo update-ca-certificates" >&2
            echo "  Or: S3_ENDPOINT=http://$s3_host" >&2
        fi
    fi
}

# --- Start local p4d ---------------------------------------------
start_p4d() {
    P4ROOT=$(mktemp -d "$TESTDIR/p4root.XXXXXX")

    # Check if S3 backend is active -- if so, start p4d with the
    # LD_PRELOAD shim so evicted-file reads are intercepted.
    local has_s3=false
    for b in "${BACKEND_LIST[@]}"; do
        [ "$b" = "s3" ] && has_s3=true
    done

    if $has_s3; then
        echo "Starting p4d with LD_PRELOAD shim (root: $P4ROOT, port: $P4PORT) ..."
        LD_PRELOAD="$P4SHIM_LIB" P4CACHE_DEPOT="$P4ROOT" \
            "$P4D_BIN" -r "$P4ROOT" -p "$P4PORT" -L "$P4ROOT/p4d.log" &
    else
        echo "Starting p4d (root: $P4ROOT, port: $P4PORT) ..."
        "$P4D_BIN" -r "$P4ROOT" -p "$P4PORT" -L "$P4ROOT/p4d.log" &
    fi
    P4D_PID=$!

    # Wait for the server to accept connections
    local retries=0
    while ! $P4BASE info >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [ $retries -gt 20 ]; then
            echo "ERROR: p4d failed to start after 10 seconds." >&2
            if [ -f "$P4ROOT/p4d.log" ]; then
                echo "--- p4d log ---" >&2
                cat "$P4ROOT/p4d.log" >&2
            fi
            exit 1
        fi
        sleep 0.5
    done
    echo "p4d ready (PID $P4D_PID)."
}

# --- Start p4-cache daemon (S3 backend only) ----------------------
start_p4cache() {
    local has_s3=false
    for b in "${BACKEND_LIST[@]}"; do
        [ "$b" = "s3" ] && has_s3=true
    done
    $has_s3 || return 0

    echo ""
    echo "Starting p4-cache daemon (depot: $P4ROOT, S3: $S3_ENDPOINT/$S3_BUCKET) ..."

    # Grant fanotify capability if not already set
    if ! getcap "$P4CACHE_BIN" 2>/dev/null | grep -q cap_sys_admin; then
        echo "  Setting CAP_SYS_ADMIN on $P4CACHE_BIN ..."
        sudo -n setcap cap_sys_admin+ep "$P4CACHE_BIN" 2>/dev/null || {
            echo "WARNING: Could not set CAP_SYS_ADMIN on $P4CACHE_BIN." >&2
            echo "  p4-cache requires fanotify. Run: sudo setcap cap_sys_admin+ep $P4CACHE_BIN" >&2
        }
    fi

    "$P4CACHE_BIN" \
        --depot-path "$P4ROOT" \
        --primary-type s3 \
        --primary-endpoint "$S3_ENDPOINT" \
        --primary-bucket "$S3_BUCKET" \
        --primary-region "$S3_REGION" \
        --primary-access-key "$S3_ACCESS_KEY" \
        --primary-secret-key "$S3_SECRET_KEY" \
        --primary-no-verify-ssl \
        --primary-prefix "$(depot_for s3)" \
        --max-cache-gb 100 \
        --upload-threads 8 \
        --restore-threads 16 \
        --verbose \
        --daemon \
        --pid-file "$P4ROOT/.p4cache.pid" \
        --log-file "$P4ROOT/p4cache.log"

    P4CACHE_PID=$(cat "$P4ROOT/.p4cache.pid" 2>/dev/null)
    echo "p4-cache ready (PID $P4CACHE_PID)."
}

# --- Create depots, user, workspaces -----------------------------
setup_p4() {
    echo ""
    echo "Setting up P4 depots, user, and workspaces ..."

    # Create user
    echo "  Creating user '$P4USER' ..."
    $P4BASE user -o "$P4USER" | $P4BASE user -i -f >/dev/null 2>&1

    for backend in "${BACKEND_LIST[@]}"; do
        local depot client ws_root depot_map
        depot=$(depot_for "$backend")
        client="ws-${backend}"
        ws_root="$TESTDIR/${client}"

        case "$backend" in
            local)
                # Local depot: relative map, files stored under p4d server root
                depot_map="${depot}/..."
                ;;

            nfs)
                # NFS depot: absolute path map pointing to NFS mount
                # p4d reads/writes to this path directly -- no symlinks
                local nfs_depot_path="${NFS_DEPOT_ROOT}/${depot}"
                if [ ! -d "$NFS_MOUNT" ]; then
                    echo "  WARNING: NFS_MOUNT=$NFS_MOUNT does not exist." >&2
                    echo "           NFS depot will fail at runtime." >&2
                fi
                mkdir -p "$nfs_depot_path" 2>/dev/null || true
                depot_map="${nfs_depot_path}/..."
                ;;

            s3)
                # S3 depot: local depot backed by p4-cache daemon.
                # p4d writes to local disk; p4-cache uploads to S3 in the background.
                depot_map="${depot}/..."
                ;;
        esac

        # Create depot
        local depot_spec
        depot_spec="Depot: ${depot}\nOwner: ${P4USER}\nType: local\nMap: ${depot_map}"
        if [ "$backend" = "s3" ]; then
            echo "  Creating depot '$depot' (backend: $backend via p4-cache, map: $depot_map)"
        else
            echo "  Creating depot '$depot' (backend: $backend, map: $depot_map)"
        fi
        printf "$depot_spec\n" | $P4BASE depot -i >/dev/null 2>&1

        # Create workspace
        mkdir -p "$ws_root"
        echo "  Creating client '$client' -> //$depot/..."
        printf "Client: %s\nOwner: %s\nRoot: %s\nOptions: noallwrite noclobber nocompress unlocked nomodtime normdir\nView:\n\t//%s/... //%s/...\n" \
            "$client" "$P4USER" "$ws_root" "$depot" "$client" | \
            $P4BASE client -i -f >/dev/null 2>&1
    done

    echo "P4 setup complete."
}

# --- Warmup -------------------------------------------------------
warmup() {
    for backend in "${BACKEND_LIST[@]}"; do
        case "$backend" in
            s3)
                echo "Warming up S3 ($S3_ENDPOINT) ..."
                s3_curl "GET" "/?list-type=2&max-keys=1" >/dev/null 2>&1 \
                    || echo "  (S3 warmup failed -- endpoint may not be reachable)"
                ;;
            nfs)
                echo "Warming up NFS ($NFS_MOUNT) ..."
                ls "$NFS_MOUNT"/ >/dev/null 2>&1 || echo "  (NFS warmup failed -- mount may not exist)"
                ;;
        esac
    done
}

# --- Timing helper ------------------------------------------------
time_cmd() {
    local start end
    start=$(date +%s%N)
    eval "$@" >/dev/null 2>&1
    local rc=$?
    end=$(date +%s%N)
    echo "scale=3; ($end - $start) / 1000000000" | bc
    return $rc
}

# --- Benchmark operations -----------------------------------------

run_test() {
    local category=$1 backend=$2 source_dir=$3 target_subdir=$4
    local client="ws-${backend}"
    local ws_root="$TESTDIR/${client}"

    local file_count total_bytes
    file_count=$(find "$source_dir" -type f | wc -l)
    total_bytes=$(du -sb "$source_dir" | cut -f1)

    rm -rf "$ws_root/$target_subdir"
    mkdir -p "$ws_root/$target_subdir"
    cp -r "$source_dir"/* "$ws_root/$target_subdir/"

    local add_time submit_time
    add_time=$(time_cmd "$P4BASE -c $client add '$ws_root/$target_subdir/...'")
    submit_time=$(time_cmd "$P4BASE -c $client submit -d 'Benchmark: $category ($backend)'")

    echo "$category,$backend,add,$file_count,$total_bytes,$add_time" >> "$RESULTS_FILE"
    echo "$category,$backend,submit,$file_count,$total_bytes,$submit_time" >> "$RESULTS_FILE"
    echo "$category,$backend,total,$file_count,$total_bytes,$(echo "$add_time + $submit_time" | bc)" >> "$RESULTS_FILE"

    printf "  %-12s %-8s  add=%.3fs  submit=%.3fs  total=%.3fs  (%d files, %s)\n" \
        "$category" "$backend" "$add_time" "$submit_time" \
        "$(echo "$add_time + $submit_time" | bc)" \
        "$file_count" "$(numfmt --to=iec "$total_bytes")"
}

run_edit_test() {
    local category=$1 backend=$2 target_subdir=$3
    local client="ws-${backend}"
    local ws_root="$TESTDIR/${client}"

    local file_count total_bytes
    file_count=$(find "$ws_root/$target_subdir" -type f | wc -l)
    total_bytes=$(du -sb "$ws_root/$target_subdir" | cut -f1)

    local edit_time submit_time
    edit_time=$(time_cmd "$P4BASE -c $client edit '$ws_root/$target_subdir/...'")

    # Ensure files are writable -- p4 edit should set +w but doesn't always
    # on every backend/platform combination.
    find "$ws_root/$target_subdir" -type f -exec chmod u+w {} +
    find "$ws_root/$target_subdir" -type f -exec sh -c 'echo x >> "$1"' _ {} \;

    submit_time=$(time_cmd "$P4BASE -c $client submit -d 'Benchmark edit: $category ($backend)'")

    echo "$category,$backend,edit,$file_count,$total_bytes,$edit_time" >> "$RESULTS_FILE"
    echo "$category,$backend,submit-edit,$file_count,$total_bytes,$submit_time" >> "$RESULTS_FILE"
    echo "$category,$backend,total-edit,$file_count,$total_bytes,$(echo "$edit_time + $submit_time" | bc)" >> "$RESULTS_FILE"

    printf "  %-12s %-8s  edit=%.3fs  submit=%.3fs  total=%.3fs  (%d files)\n" \
        "$category" "$backend" "$edit_time" "$submit_time" \
        "$(echo "$edit_time + $submit_time" | bc)" \
        "$file_count"
}

run_sync_test() {
    local category=$1 backend=$2 target_subdir=$3
    local client="ws-${backend}"
    local ws_root="$TESTDIR/${client}"

    local file_count
    file_count=$($P4BASE -c "$client" fstat "//${client}/$target_subdir/..." 2>/dev/null | grep -c depotFile || true)
    file_count=${file_count:-0}

    # Revert any files left open by a previous test (e.g. failed edit)
    # so they don't block the sync.
    $P4BASE -c "$client" revert "//${client}/$target_subdir/..." >/dev/null 2>&1 || true
    $P4BASE -c "$client" sync -f "$ws_root/$target_subdir/...#none" >/dev/null 2>&1 || true
    rm -rf "$ws_root/$target_subdir"
    mkdir -p "$ws_root/$target_subdir"

    local sync_time total_bytes
    sync_time=$(time_cmd "$P4BASE -c $client sync -f '//${client}/$target_subdir/...'")
    total_bytes=$(du -sb "$ws_root/$target_subdir" 2>/dev/null | cut -f1 || true)
    total_bytes=${total_bytes:-0}

    echo "$category,$backend,sync,$file_count,$total_bytes,$sync_time" >> "$RESULTS_FILE"

    printf "  %-12s %-8s  sync=%.3fs  (%d files, %s)\n" \
        "$category" "$backend" "$sync_time" "$file_count" \
        "$(numfmt --to=iec "${total_bytes:-0}")"
}

# Run add + edit + sync for one file-size category across all backends
run_category() {
    local label=$1 source_dir=$2 subdir=$3 desc=$4

    echo "--- ${desc}: add+submit ---"
    for backend in "${BACKEND_LIST[@]}"; do
        run_test "${label}-add" "$backend" "$source_dir" "$subdir"
    done
    echo ""

    echo "--- ${desc}: edit+submit ---"
    for backend in "${BACKEND_LIST[@]}"; do
        run_edit_test "${label}-edit" "$backend" "$subdir"
    done
    echo ""

    echo "--- ${desc}: sync ---"
    for backend in "${BACKEND_LIST[@]}"; do
        run_sync_test "${label}-sync" "$backend" "$subdir"
    done
    echo ""
}

# ============================================================
# Main
# ============================================================

echo "================================================="
echo "P4 Storage Backend Benchmark"
echo "================================================="
echo ""
echo "P4:       $P4"
echo "P4D:      $P4D_BIN"
echo "Port:     $P4PORT"
echo "User:     $P4USER"
echo "Backends: $BACKENDS"
echo "Workdir:  $TESTDIR"
for b in "${BACKEND_LIST[@]}"; do
    case "$b" in
        local) echo "  local:  p4d server root (local disk)  ->  //$(depot_for local)/..." ;;
        s3)    echo "  s3:     p4-cache -> $S3_ENDPOINT/$S3_BUCKET  ->  //$(depot_for s3)/..." ;;
        nfs)   echo "  nfs:    $NFS_DEPOT_ROOT  ->  //$(depot_for nfs)/..." ;;
    esac
done
for b in "${BACKEND_LIST[@]}"; do
    if [ "$b" = "s3" ]; then
        echo "p4-cache: $P4CACHE_BIN"
        echo "shim:     $P4SHIM_LIB"
        break
    fi
done
echo ""

# -- Set up S3 SSL trust (before cleanup so curl can reach S3) -----
setup_s3_ssl

# -- Clean up stale data from previous runs ------------------------
echo "Cleaning up stale data from previous runs ..."
kill_stale_p4d
clean_test_artifacts
echo ""

# -- Generate test data --------------------------------------------
generate_testdata
echo ""

# -- Start p4d and configure ---------------------------------------
start_p4d
start_p4cache
setup_p4

echo ""
echo "category,backend,operation,files,total_bytes,elapsed_sec" > "$RESULTS_FILE"

# -- Warmup --------------------------------------------------------
warmup
echo ""

# -- Benchmarks ----------------------------------------------------
DATADIR=$TESTDIR/testdata

run_category "small"  "$DATADIR/small"  "small"  "Small files (100 x 1KB = 100KB)"
run_category "medium" "$DATADIR/medium" "medium" "Medium files (50 x 100KB = 5MB)"
run_category "large"  "$DATADIR/large"  "large"  "Large files (20 x 1MB = 20MB)"
run_category "xlarge" "$DATADIR/xlarge" "xlarge" "XLarge files (5 x 10MB = 50MB)"
run_category "huge"   "$DATADIR/huge"   "huge"   "Huge files (5 x 100MB = 500MB)"

# Build combined test data (excludes huge to keep mixed comparable)
COMBINED=$DATADIR/combined
rm -rf "$COMBINED"
mkdir -p "$COMBINED"
cp -r "$DATADIR/small"/*  "$COMBINED/"
cp -r "$DATADIR/medium"/* "$COMBINED/"
cp -r "$DATADIR/large"/*  "$COMBINED/"
cp -r "$DATADIR/xlarge"/* "$COMBINED/"

run_category "mixed" "$COMBINED" "mixed" "Mixed workload (175 files, ~76MB)"

# -- Summary -------------------------------------------------------
echo "================================================="
echo "Benchmark complete.  Results in $RESULTS_FILE"
echo "================================================="

# Check logs for S3 errors
echo ""
S3_ERRORS=0
for logfile in "$P4ROOT/p4d.log" "$P4ROOT/p4cache.log"; do
    if [ -f "$logfile" ]; then
        local_errors=$(grep -ci "s3.*fail\|s3.*error\|ssl.*error\|certificate\|upload.*fail" "$logfile" 2>/dev/null || true)
        S3_ERRORS=$((S3_ERRORS + ${local_errors:-0}))
    fi
done
if [ "$S3_ERRORS" -gt 0 ] 2>/dev/null; then
    echo "WARNING: $S3_ERRORS S3/SSL errors detected in logs."
    echo "  First few errors:"
    grep -i "s3.*fail\|s3.*error\|ssl.*error\|certificate\|upload.*fail" \
        "$P4ROOT/p4d.log" "$P4ROOT/p4cache.log" 2>/dev/null | head -5 | sed 's/^/    /'
    echo "  Logs saved to $TESTDIR/p4d.log and $TESTDIR/p4cache.log"
else
    echo "No S3 errors detected."
fi
