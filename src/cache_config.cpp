#include "p4cache/cache_config.hpp"

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

namespace p4cache {

// --- BackendConfig ---

std::string BackendConfig::validate() const {
    if (type.empty()) return "backend type is required";
    if (type == "s3") {
        if (params.count("bucket") == 0 || params.at("bucket").empty())
            return "s3 backend requires 'bucket'";
    } else if (type == "azure") {
        if (params.count("container") == 0 || params.at("container").empty())
            return "azure backend requires 'container'";
    } else if (type == "gcs") {
        if (params.count("bucket") == 0 || params.at("bucket").empty())
            return "gcs backend requires 'bucket'";
    } else if (type == "nfs") {
        if (params.count("path") == 0 || params.at("path").empty())
            return "nfs backend requires 'path'";
        if (!std::filesystem::exists(params.at("path")))
            return "nfs backend path does not exist: " + params.at("path");
    } else {
        return "unknown backend type: " + type;
    }
    return {};
}

// --- CacheConfig ---

namespace {

// Parse a --primary-X or --secondary-X flag and route to the correct BackendConfig.
// Returns true if the flag was handled, false if it wasn't a backend flag.
bool parse_backend_flag(const std::string& arg, const char* value,
                        BackendConfig& primary, BackendConfig& secondary) {
    // Determine prefix and target
    BackendConfig* target = nullptr;
    std::string suffix;

    if (arg.compare(0, 10, "--primary-") == 0) {
        target = &primary;
        suffix = arg.substr(10);
    } else if (arg.compare(0, 12, "--secondary-") == 0) {
        target = &secondary;
        suffix = arg.substr(12);
    } else {
        return false;
    }

    // Map CLI suffix to BackendConfig field
    if (suffix == "type") {
        target->type = value;
    } else if (suffix == "endpoint") {
        target->params["endpoint"] = value;
    } else if (suffix == "bucket") {
        target->params["bucket"] = value;
    } else if (suffix == "container") {
        target->params["container"] = value;
    } else if (suffix == "region") {
        target->params["region"] = value;
    } else if (suffix == "access-key") {
        target->params["access_key"] = value;
    } else if (suffix == "secret-key") {
        target->params["secret_key"] = value;
    } else if (suffix == "account-name") {
        target->params["account_name"] = value;
    } else if (suffix == "account-key") {
        target->params["account_key"] = value;
    } else if (suffix == "sas-token") {
        target->params["sas_token"] = value;
    } else if (suffix == "project-id") {
        target->params["project_id"] = value;
    } else if (suffix == "credentials-file") {
        target->params["credentials_file"] = value;
    } else if (suffix == "path") {
        target->params["path"] = value;
    } else if (suffix == "prefix") {
        target->params["path_prefix"] = value;
    } else if (suffix == "ca-cert") {
        target->params["ca_cert_path"] = value;
    } else {
        return false;
    }
    return true;
}

// Parse boolean backend flags (no value argument needed)
bool parse_backend_bool_flag(const std::string& arg,
                             BackendConfig& primary, BackendConfig& secondary) {
    BackendConfig* target = nullptr;
    std::string suffix;

    if (arg.compare(0, 10, "--primary-") == 0) {
        target = &primary;
        suffix = arg.substr(10);
    } else if (arg.compare(0, 12, "--secondary-") == 0) {
        target = &secondary;
        suffix = arg.substr(12);
    } else {
        return false;
    }

    if (suffix == "no-verify-ssl") {
        target->params["verify_ssl"] = "false";
    } else if (suffix == "sse") {
        target->params["server_side_encryption"] = "true";
    } else {
        return false;
    }
    return true;
}

// Check if a backend flag requires a value argument
bool is_backend_bool_flag(const std::string& arg) {
    auto check = [&](const std::string& prefix) -> bool {
        if (arg.compare(0, prefix.size(), prefix) != 0) return false;
        std::string suffix = arg.substr(prefix.size());
        return suffix == "no-verify-ssl" || suffix == "sse";
    };
    return check("--primary-") || check("--secondary-");
}

}  // namespace

std::optional<CacheConfig> CacheConfig::from_args(int argc, char* argv[]) {
    CacheConfig config;

    auto next_arg = [&](int& i, const char* name) -> const char* {
        if (i + 1 >= argc) {
            std::cerr << "Error: " << name << " requires an argument\n";
            return nullptr;
        }
        return argv[++i];
    };

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        // Check for boolean backend flags first (no value needed)
        if (is_backend_bool_flag(arg)) {
            if (!parse_backend_bool_flag(arg, config.primary, config.secondary)) {
                std::cerr << "Error: unknown option: " << arg << "\n";
                return std::nullopt;
            }
            continue;
        }

        // Check for backend flags with values (--primary-X / --secondary-X)
        if (arg.compare(0, 10, "--primary-") == 0 || arg.compare(0, 12, "--secondary-") == 0) {
            auto* v = next_arg(i, arg.c_str());
            if (!v) return std::nullopt;
            if (!parse_backend_flag(arg, v, config.primary, config.secondary)) {
                std::cerr << "Error: unknown option: " << arg << "\n";
                return std::nullopt;
            }
            continue;
        }

        if (arg == "--depot-path") {
            auto* v = next_arg(i, "--depot-path");
            if (!v) return std::nullopt;
            config.depot_path = v;
        } else if (arg == "--read-only") {
            config.read_only = true;
        } else if (arg == "--max-cache-gb") {
            auto* v = next_arg(i, "--max-cache-gb");
            if (!v) return std::nullopt;
            config.max_cache_bytes = std::stoull(v) * 1024ULL * 1024 * 1024;
        } else if (arg == "--max-cache-bytes") {
            auto* v = next_arg(i, "--max-cache-bytes");
            if (!v) return std::nullopt;
            config.max_cache_bytes = std::stoull(v);
        } else if (arg == "--low-watermark-gb") {
            auto* v = next_arg(i, "--low-watermark-gb");
            if (!v) return std::nullopt;
            config.eviction_low_watermark = std::stoull(v) * 1024ULL * 1024 * 1024;
        } else if (arg == "--low-watermark-bytes") {
            auto* v = next_arg(i, "--low-watermark-bytes");
            if (!v) return std::nullopt;
            config.eviction_low_watermark = std::stoull(v);
        } else if (arg == "--upload-threads") {
            auto* v = next_arg(i, "--upload-threads");
            if (!v) return std::nullopt;
            config.upload_threads = std::stoull(v);
        } else if (arg == "--restore-threads") {
            auto* v = next_arg(i, "--restore-threads");
            if (!v) return std::nullopt;
            config.restore_threads = std::stoull(v);
        } else if (arg == "--config") {
            auto* v = next_arg(i, "--config");
            if (!v) return std::nullopt;
            if (!config.load_json(v)) return std::nullopt;
        } else if (arg == "--skip-startup-scan") {
            config.skip_startup_scan = true;
        } else if (arg == "--daemon") {
            config.daemonize = true;
        } else if (arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--pid-file") {
            auto* v = next_arg(i, "--pid-file");
            if (!v) return std::nullopt;
            config.pid_file = v;
        } else if (arg == "--log-file") {
            auto* v = next_arg(i, "--log-file");
            if (!v) return std::nullopt;
            config.log_file = v;
        } else if (arg == "--stats-interval") {
            auto* v = next_arg(i, "--stats-interval");
            if (!v) return std::nullopt;
            config.stats_interval_secs = std::stoull(v);
        } else if (arg == "--metrics-file") {
            auto* v = next_arg(i, "--metrics-file");
            if (!v) return std::nullopt;
            config.metrics_file = v;
        } else if (arg == "--metrics-interval") {
            auto* v = next_arg(i, "--metrics-interval");
            if (!v) return std::nullopt;
            config.metrics_interval_secs = std::stoull(v);
        } else if (arg == "--no-access-log") {
            config.access_log_enabled = false;
        } else if (arg == "--access-batch-size") {
            auto* v = next_arg(i, "--access-batch-size");
            if (!v) return std::nullopt;
            config.access_batch_size = std::stoull(v);
        } else if (arg == "--access-mapsize-gb") {
            auto* v = next_arg(i, "--access-mapsize-gb");
            if (!v) return std::nullopt;
            config.access_mapsize_gb = std::stoull(v);
        } else if (arg == "--access-db-path") {
            auto* v = next_arg(i, "--access-db-path");
            if (!v) return std::nullopt;
            config.access_db_path = v;
        } else if (arg == "--help" || arg == "-h") {
            std::cerr <<
                "Usage: p4-cache --depot-path <path> --primary-type <s3|azure|gcs|nfs> [options]\n"
                "\n"
                "Required:\n"
                "  --depot-path <path>              P4 depot directory on NVMe\n"
                "  --primary-type <type>            Primary backend: s3, azure, gcs, nfs\n"
                "\n"
                "Mode:\n"
                "  --read-only                      Read-only mode for replica servers.\n"
                "                                   No uploads, only cache reads.\n"
                "\n"
                "Primary backend (--primary-*):\n"
                "  --primary-endpoint <url>         Endpoint URL (S3/Azure/GCS)\n"
                "  --primary-bucket <name>          Bucket name (S3/GCS)\n"
                "  --primary-container <name>       Container name (Azure)\n"
                "  --primary-region <region>        Region (S3, default: us-east-1)\n"
                "  --primary-access-key <key>       Access key (S3, or AWS_ACCESS_KEY_ID env)\n"
                "  --primary-secret-key <key>       Secret key (S3, or AWS_SECRET_ACCESS_KEY env)\n"
                "  --primary-account-name <name>    Account name (Azure)\n"
                "  --primary-account-key <key>      Account key (Azure)\n"
                "  --primary-sas-token <token>      SAS token (Azure)\n"
                "  --primary-project-id <id>        Project ID (GCS)\n"
                "  --primary-credentials-file <p>   Credentials file (GCS)\n"
                "  --primary-path <path>            Local path (NFS)\n"
                "  --primary-prefix <prefix>        Key prefix (default: depot dir name)\n"
                "  --primary-ca-cert <path>         CA certificate for SSL\n"
                "  --primary-no-verify-ssl          Skip SSL verification\n"
                "  --primary-sse                    Enable server-side encryption\n"
                "\n"
                "Secondary backend (--secondary-*, optional read-only fallback):\n"
                "  Same flags as primary with --secondary- prefix.\n"
                "\n"
                "Cache options:\n"
                "  --config <path>                  JSON config file\n"
                "  --max-cache-gb <N>               Max cache size in GB (default: 100)\n"
                "  --low-watermark-gb <N>           Eviction threshold in GB (default: 80)\n"
                "  --upload-threads <N>             Upload worker threads (default: 8)\n"
                "  --restore-threads <N>            Restore worker threads (default: 16)\n"
                "  --daemon                         Run as daemon\n"
                "  --verbose                        Verbose output\n"
                "  --pid-file <path>                PID file path\n"
                "  --log-file <path>                Log file path\n"
                "  --skip-startup-scan              Skip scanning for untracked files on startup\n"
                "  --stats-interval <secs>          Stats reporting interval (default: 60)\n"
                "  --metrics-file <path>            Prometheus .prom file for node_exporter textfile collector\n"
                "  --metrics-interval <secs>        Metrics write interval (default: 15)\n"
                "\n"
                "Access log:\n"
                "  --no-access-log                  Disable access log (default: enabled)\n"
                "  --access-batch-size <N>          Access log batch size (default: 10000)\n"
                "  --access-mapsize-gb <N>          Access log LMDB map size in GB (default: 512)\n"
                "  --access-db-path <path>          Access log LMDB directory (default: <state_dir>/access/)\n"
                "                                   Use to share one access DB across multiple daemons.\n"
                "  --help                           Show this help\n";
            return std::nullopt;
        } else {
            std::cerr << "Error: unknown option: " << arg << "\n";
            return std::nullopt;
        }
    }

    // Load S3 credentials from environment if primary is S3 and not set on CLI
    if (config.primary.type == "s3") {
        if (config.primary.params.count("access_key") == 0 || config.primary.params["access_key"].empty()) {
            if (const char* v = std::getenv("AWS_ACCESS_KEY_ID")) {
                config.primary.params["access_key"] = v;
            }
        }
        if (config.primary.params.count("secret_key") == 0 || config.primary.params["secret_key"].empty()) {
            if (const char* v = std::getenv("AWS_SECRET_ACCESS_KEY")) {
                config.primary.params["secret_key"] = v;
            }
        }
    }

    config.apply_defaults();
    return config;
}

bool CacheConfig::load_json(const std::filesystem::path& path) {
    try {
        std::ifstream ifs(path);
        if (!ifs) {
            std::cerr << "Error: cannot open config file: " << path << "\n";
            return false;
        }
        auto j = nlohmann::json::parse(ifs);

        if (j.contains("depot_path")) depot_path = j["depot_path"].get<std::string>();
        if (j.contains("read_only")) read_only = j["read_only"].get<bool>();
        if (j.contains("max_cache_gb"))
            max_cache_bytes = j["max_cache_gb"].get<uint64_t>() * 1024ULL * 1024 * 1024;
        if (j.contains("low_watermark_gb"))
            eviction_low_watermark = j["low_watermark_gb"].get<uint64_t>() * 1024ULL * 1024 * 1024;
        if (j.contains("upload_threads")) upload_threads = j["upload_threads"].get<size_t>();
        if (j.contains("upload_concurrency")) upload_concurrency = j["upload_concurrency"].get<size_t>();
        if (j.contains("restore_threads")) restore_threads = j["restore_threads"].get<size_t>();
        if (j.contains("verbose")) verbose = j["verbose"].get<bool>();
        if (j.contains("skip_startup_scan")) skip_startup_scan = j["skip_startup_scan"].get<bool>();
        if (j.contains("stats_interval")) stats_interval_secs = j["stats_interval"].get<size_t>();
        if (j.contains("metrics_file")) metrics_file = j["metrics_file"].get<std::string>();
        if (j.contains("metrics_interval")) metrics_interval_secs = j["metrics_interval"].get<size_t>();
        if (j.contains("access_log_enabled")) access_log_enabled = j["access_log_enabled"].get<bool>();
        if (j.contains("access_batch_size")) access_batch_size = j["access_batch_size"].get<size_t>();
        if (j.contains("access_mapsize_gb")) access_mapsize_gb = j["access_mapsize_gb"].get<uint64_t>();
        if (j.contains("access_db_path")) access_db_path = j["access_db_path"].get<std::string>();

        // Parse primary backend
        if (j.contains("primary") && j["primary"].is_object()) {
            auto& jp = j["primary"];
            if (jp.contains("type")) primary.type = jp["type"].get<std::string>();
            for (auto& [key, val] : jp.items()) {
                if (key != "type") {
                    primary.params[key] = val.get<std::string>();
                }
            }
        }

        // Parse secondary backend
        if (j.contains("secondary") && j["secondary"].is_object()) {
            auto& js = j["secondary"];
            if (js.contains("type")) secondary.type = js["type"].get<std::string>();
            for (auto& [key, val] : js.items()) {
                if (key != "type") {
                    secondary.params[key] = val.get<std::string>();
                }
            }
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error parsing config: " << e.what() << "\n";
        return false;
    }
}

void CacheConfig::apply_defaults() {
    if (state_dir.empty() && !depot_path.empty()) {
        state_dir = depot_path / ".p4cache";
    }

    // Default path_prefix for cloud backends: depot directory basename
    auto set_default_prefix = [&](BackendConfig& bc) {
        if (!bc.empty() && bc.type != "nfs" &&
            (bc.params.count("path_prefix") == 0 || bc.params["path_prefix"].empty()) &&
            !depot_path.empty()) {
            bc.params["path_prefix"] = depot_path.filename().string();
        }
    };
    set_default_prefix(primary);
    set_default_prefix(secondary);

    // Recompute eviction target as 90% of low watermark
    eviction_target = eviction_low_watermark * 9 / 10;
}

std::string CacheConfig::validate() const {
    if (depot_path.empty()) return "depot_path is required (--depot-path)";
    if (!std::filesystem::exists(depot_path)) return "depot_path does not exist: " + depot_path.string();
    if (!std::filesystem::is_directory(depot_path)) return "depot_path is not a directory: " + depot_path.string();
    if (primary.empty()) return "primary backend type is required (--primary-type)";
    auto err = primary.validate();
    if (!err.empty()) return "primary: " + err;
    if (!secondary.empty()) {
        err = secondary.validate();
        if (!err.empty()) return "secondary: " + err;
    }
    if (max_cache_bytes == 0) return "max_cache_bytes must be > 0";
    if (eviction_low_watermark > max_cache_bytes) return "low_watermark must be <= max_cache_bytes";
    return {};
}

}  // namespace p4cache
