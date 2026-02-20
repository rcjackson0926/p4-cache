// p4-cache-access: Standalone query tool for the p4-cache access log.
//
// Opens the access LMDB database read-only. Only links against LMDB.
//
// Usage: p4-cache-access --db <path> <subcommand> [args]
//
// Subcommands:
//   stat                         Entry count, tree depth, DB size
//   count                        Total entries (fast)
//   get <path>                   Look up last access time for one file
//   prefix <prefix>              List files under a directory prefix
//   stale --before <time>        Files not accessed since a given time
//   export [--format csv|tsv]    Dump entire database

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <lmdb.h>
#include <string>

namespace {

/// Parse a duration string like "30d", "6h", "90m", "3600s" or a raw epoch.
/// Returns epoch timestamp (seconds since epoch).
uint64_t parse_before_time(const char* arg) {
    size_t len = strlen(arg);
    if (len == 0) {
        fprintf(stderr, "Error: empty --before value\n");
        exit(1);
    }

    char unit = arg[len - 1];
    if (unit == 'd' || unit == 'h' || unit == 'm' || unit == 's') {
        uint64_t val = strtoull(arg, nullptr, 10);
        uint64_t seconds = 0;
        switch (unit) {
            case 'd': seconds = val * 86400; break;
            case 'h': seconds = val * 3600; break;
            case 'm': seconds = val * 60; break;
            case 's': seconds = val; break;
        }
        auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        return static_cast<uint64_t>(now) - seconds;
    }

    // Raw epoch seconds
    return strtoull(arg, nullptr, 10);
}

void format_timestamp(uint64_t ts, char* buf, size_t buf_size) {
    time_t t = static_cast<time_t>(ts);
    struct tm tm_val;
    gmtime_r(&t, &tm_val);
    strftime(buf, buf_size, "%Y-%m-%dT%H:%M:%SZ", &tm_val);
}

void print_usage() {
    fprintf(stderr,
        "Usage: p4-cache-access --db <path> <subcommand> [args]\n"
        "\n"
        "Subcommands:\n"
        "  stat                          Entry count, tree depth, DB size\n"
        "  count                         Total entries (fast)\n"
        "  get <path>                    Look up last access time for one file\n"
        "  prefix <prefix>               List files under a directory prefix\n"
        "  stale --before <time>         Files not accessed since a given time\n"
        "  export [--format csv|tsv]     Dump entire database\n"
        "\n"
        "Options:\n"
        "  --db <path>                   Path to access LMDB directory\n"
        "                                (default: $P4CACHE_DEPOT/.p4cache/access/\n"
        "                                 or .p4cache/access/ under CWD)\n"
        "  --before <time>               Epoch seconds or duration (30d, 6h, 90m)\n"
        "  --limit <N>                   Max entries to output\n"
        "  --output <file>               Write output to file (uses 1MB buffer)\n"
        "  --help                        Show this help\n"
    );
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string db_path;
    std::string subcommand;
    std::string get_path;
    std::string prefix_str;
    std::string before_str;
    std::string format_str = "tsv";
    std::string output_file;
    uint64_t limit = 0;

    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--db") {
            if (++i >= argc) { fprintf(stderr, "--db requires argument\n"); return 1; }
            db_path = argv[i];
        } else if (arg == "--before") {
            if (++i >= argc) { fprintf(stderr, "--before requires argument\n"); return 1; }
            before_str = argv[i];
        } else if (arg == "--limit") {
            if (++i >= argc) { fprintf(stderr, "--limit requires argument\n"); return 1; }
            limit = strtoull(argv[i], nullptr, 10);
        } else if (arg == "--format") {
            if (++i >= argc) { fprintf(stderr, "--format requires argument\n"); return 1; }
            format_str = argv[i];
        } else if (arg == "--output") {
            if (++i >= argc) { fprintf(stderr, "--output requires argument\n"); return 1; }
            output_file = argv[i];
        } else if (arg == "--help" || arg == "-h") {
            print_usage();
            return 0;
        } else if (arg[0] != '-' && subcommand.empty()) {
            subcommand = arg;
        } else if (subcommand == "get" && get_path.empty()) {
            get_path = arg;
        } else if (subcommand == "prefix" && prefix_str.empty()) {
            prefix_str = arg;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", arg.c_str());
            print_usage();
            return 1;
        }
    }

    if (subcommand.empty()) {
        print_usage();
        return 1;
    }

    // Resolve default db path
    if (db_path.empty()) {
        const char* depot_env = getenv("P4CACHE_DEPOT");
        if (depot_env) {
            db_path = std::string(depot_env) + "/.p4cache/access";
        } else {
            db_path = ".p4cache/access";
        }
    }

    // Open LMDB read-only
    MDB_env* env = nullptr;
    int rc = mdb_env_create(&env);
    if (rc) {
        fprintf(stderr, "mdb_env_create: %s\n", mdb_strerror(rc));
        return 1;
    }

    // Read-only doesn't need a large mapsize, but we set it to match
    rc = mdb_env_set_mapsize(env, 512ULL * 1024 * 1024 * 1024);
    if (rc) {
        fprintf(stderr, "mdb_env_set_mapsize: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        return 1;
    }

    rc = mdb_env_open(env, db_path.c_str(), MDB_RDONLY | MDB_NOSUBDIR * 0, 0664);
    if (rc) {
        fprintf(stderr, "Cannot open access DB at %s: %s\n", db_path.c_str(), mdb_strerror(rc));
        mdb_env_close(env);
        return 1;
    }

    MDB_txn* txn = nullptr;
    rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
    if (rc) {
        fprintf(stderr, "mdb_txn_begin: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        return 1;
    }

    MDB_dbi dbi;
    rc = mdb_dbi_open(txn, nullptr, 0, &dbi);
    if (rc) {
        fprintf(stderr, "mdb_dbi_open: %s\n", mdb_strerror(rc));
        mdb_txn_abort(txn);
        mdb_env_close(env);
        return 1;
    }

    // Set up output file if specified
    FILE* out = stdout;
    if (!output_file.empty()) {
        out = fopen(output_file.c_str(), "w");
        if (!out) {
            fprintf(stderr, "Cannot open output file: %s\n", output_file.c_str());
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return 1;
        }
        // Set 1MB write buffer
        setvbuf(out, nullptr, _IOFBF, 1024 * 1024);
    }

    char time_buf[64];
    const char* sep = (format_str == "csv") ? "," : "\t";

    // --- Subcommands ---

    if (subcommand == "stat") {
        MDB_stat stat;
        rc = mdb_stat(txn, dbi, &stat);
        if (rc) {
            fprintf(stderr, "mdb_stat: %s\n", mdb_strerror(rc));
        } else {
            MDB_envinfo info;
            mdb_env_info(env, &info);

            fprintf(out, "entries:     %zu\n", stat.ms_entries);
            fprintf(out, "depth:       %u\n", stat.ms_depth);
            fprintf(out, "page_size:   %u\n", stat.ms_psize);
            fprintf(out, "branch_pgs:  %zu\n", stat.ms_branch_pages);
            fprintf(out, "leaf_pgs:    %zu\n", stat.ms_leaf_pages);
            fprintf(out, "overflow_pgs:%zu\n", stat.ms_overflow_pages);
            uint64_t total_pages = stat.ms_branch_pages + stat.ms_leaf_pages + stat.ms_overflow_pages;
            uint64_t db_bytes = total_pages * stat.ms_psize;
            fprintf(out, "db_size:     %" PRIu64 " bytes (%.2f MB)\n",
                    db_bytes, static_cast<double>(db_bytes) / (1024.0 * 1024));
            fprintf(out, "map_size:    %" PRIu64 " bytes (%.2f GB)\n",
                    static_cast<uint64_t>(info.me_mapsize),
                    static_cast<double>(info.me_mapsize) / (1024.0 * 1024 * 1024));
        }
    } else if (subcommand == "count") {
        MDB_stat stat;
        rc = mdb_stat(txn, dbi, &stat);
        if (rc) {
            fprintf(stderr, "mdb_stat: %s\n", mdb_strerror(rc));
        } else {
            fprintf(out, "%zu\n", stat.ms_entries);
        }
    } else if (subcommand == "get") {
        if (get_path.empty()) {
            fprintf(stderr, "Usage: p4-cache-access get <path>\n");
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return 1;
        }

        MDB_val k = {get_path.size(), const_cast<char*>(get_path.data())};
        MDB_val v;
        rc = mdb_get(txn, dbi, &k, &v);
        if (rc == MDB_NOTFOUND) {
            fprintf(out, "NOTFOUND\n");
        } else if (rc) {
            fprintf(stderr, "mdb_get: %s\n", mdb_strerror(rc));
        } else if (v.mv_size >= 8) {
            uint64_t ts;
            memcpy(&ts, v.mv_data, sizeof(ts));
            format_timestamp(ts, time_buf, sizeof(time_buf));
            fprintf(out, "%" PRIu64 "\t%s\n", ts, time_buf);
        }
    } else if (subcommand == "prefix") {
        if (prefix_str.empty()) {
            fprintf(stderr, "Usage: p4-cache-access prefix <prefix>\n");
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return 1;
        }

        MDB_cursor* cursor = nullptr;
        rc = mdb_cursor_open(txn, dbi, &cursor);
        if (rc) {
            fprintf(stderr, "mdb_cursor_open: %s\n", mdb_strerror(rc));
        } else {
            MDB_val k = {prefix_str.size(), const_cast<char*>(prefix_str.data())};
            MDB_val v;
            uint64_t count = 0;

            rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
            while (rc == 0) {
                std::string path(static_cast<const char*>(k.mv_data), k.mv_size);
                if (path.compare(0, prefix_str.size(), prefix_str) != 0) break;

                if (v.mv_size >= 8) {
                    uint64_t ts;
                    memcpy(&ts, v.mv_data, sizeof(ts));
                    format_timestamp(ts, time_buf, sizeof(time_buf));
                    fprintf(out, "%s%s%" PRIu64 "%s%s\n", path.c_str(), sep, ts, sep, time_buf);
                }

                ++count;
                if (limit > 0 && count >= limit) break;
                rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
            }
            mdb_cursor_close(cursor);
        }
    } else if (subcommand == "stale") {
        if (before_str.empty()) {
            fprintf(stderr, "Usage: p4-cache-access stale --before <time>\n");
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return 1;
        }

        uint64_t threshold = parse_before_time(before_str.c_str());

        MDB_cursor* cursor = nullptr;
        rc = mdb_cursor_open(txn, dbi, &cursor);
        if (rc) {
            fprintf(stderr, "mdb_cursor_open: %s\n", mdb_strerror(rc));
        } else {
            MDB_val k, v;
            uint64_t count = 0;
            uint64_t scanned = 0;

            rc = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            while (rc == 0) {
                ++scanned;
                if (scanned % 10000000 == 0) {
                    fprintf(stderr, "\rScanned %" PRIu64 "M entries...", scanned / 1000000);
                }

                if (v.mv_size >= 8) {
                    uint64_t ts;
                    memcpy(&ts, v.mv_data, sizeof(ts));
                    if (ts < threshold) {
                        std::string path(static_cast<const char*>(k.mv_data), k.mv_size);
                        format_timestamp(ts, time_buf, sizeof(time_buf));
                        fprintf(out, "%s%s%" PRIu64 "%s%s\n", path.c_str(), sep, ts, sep, time_buf);
                        ++count;
                        if (limit > 0 && count >= limit) break;
                    }
                }

                rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
            }
            if (scanned >= 10000000) {
                fprintf(stderr, "\rScanned %" PRIu64 " entries total\n", scanned);
            }
            mdb_cursor_close(cursor);
        }
    } else if (subcommand == "export") {
        MDB_cursor* cursor = nullptr;
        rc = mdb_cursor_open(txn, dbi, &cursor);
        if (rc) {
            fprintf(stderr, "mdb_cursor_open: %s\n", mdb_strerror(rc));
        } else {
            MDB_val k, v;
            uint64_t count = 0;

            rc = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            while (rc == 0) {
                ++count;
                if (count % 10000000 == 0) {
                    fprintf(stderr, "\rExported %" PRIu64 "M entries...", count / 1000000);
                }

                if (v.mv_size >= 8) {
                    uint64_t ts;
                    memcpy(&ts, v.mv_data, sizeof(ts));
                    std::string path(static_cast<const char*>(k.mv_data), k.mv_size);
                    format_timestamp(ts, time_buf, sizeof(time_buf));
                    fprintf(out, "%s%s%" PRIu64 "%s%s\n", path.c_str(), sep, ts, sep, time_buf);
                }

                if (limit > 0 && count >= limit) break;
                rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
            }
            if (count >= 10000000) {
                fprintf(stderr, "\rExported %" PRIu64 " entries total\n", count);
            }
            mdb_cursor_close(cursor);
        }
    } else {
        fprintf(stderr, "Unknown subcommand: %s\n", subcommand.c_str());
        print_usage();
        mdb_txn_abort(txn);
        mdb_env_close(env);
        if (out != stdout) fclose(out);
        return 1;
    }

    if (out != stdout) fclose(out);
    mdb_txn_abort(txn);
    mdb_env_close(env);
    return 0;
}
