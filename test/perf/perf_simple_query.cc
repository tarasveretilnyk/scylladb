/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/assert.hh"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <json/json.h>
#include <fmt/ranges.h>

#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/testing/test_runner.hh>
#include "test/lib/random_utils.hh"
#include "db/config.hh"

#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/tags/extension.hh"
#include "gms/gossiper.hh"
#include "types/set.hh"

static const sstring table_name = "cf";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

// Build a comma-separated column list like: "C0", "C1", "C2"
static sstring make_column_list(unsigned cell_count) {
    sstring result;
    for (unsigned i = 0; i < cell_count; ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += format("\"C{}\"", i);
    }
    return result;
}

// Generate a deterministic hex string representing a blob of the given size.
static sstring make_blob_hex(unsigned cell_size) {
    bytes b(bytes::initialized_later(), cell_size);
    for (unsigned i = 0; i < cell_size; ++i) {
        b[i] = static_cast<int8_t>(i & 0xff);
    }
    return to_hex(b);
}

// Build SET assignments for an UPDATE with blob values, e.g.:
// "C0" = 0xabcd, "C1" = 0xabcd
static sstring make_update_set_clause(unsigned cell_count, const sstring& blob_hex) {
    sstring result;
    for (unsigned i = 0; i < cell_count; ++i) {
        if (i > 0) {
            result += ",";
        }
        result += format("\"C{}\" = 0x{}", i, blob_hex);
    }
    return result;
}

// Build SET assignments for a counter UPDATE, e.g.:
// "C0" = "C0" + 1, "C1" = "C1" + 2
static sstring make_counter_set_clause(unsigned cell_count) {
    sstring result;
    for (unsigned i = 0; i < cell_count; ++i) {
        if (i > 0) {
            result += ",";
        }
        result += format("\"C{0}\" = \"C{0}\" + {1}", i, i + 1);
    }
    return result;
}

// Generate a CQL set literal like {0, 1, 2, ..., N-1}
static sstring make_set_literal(unsigned collection_size) {
    sstring result = "{";
    for (unsigned i = 0; i < collection_size; ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += format("{}", i);
    }
    result += "}";
    return result;
}

struct test_config {
    enum class run_mode { read, write, del };
    run_mode mode;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
    bool counters;
    bool flush_memtables;
    unsigned memtable_partitions = 0;
    unsigned operations_per_shard = 0;
    bool stop_on_error;
    sstring timeout;
    bool bypass_cache;
    std::optional<unsigned> initial_tablets;
    unsigned cell_count = 5;
    unsigned cell_size = 34;
    bool collection_column = false;
    unsigned collection_size = 10;
};

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << ", cell_count=" << cfg.cell_count
           << ", cell_size=" << cfg.cell_size;
    if (cfg.collection_column) {
        os << ", collection_column=yes"
           << ", collection_size=" << cfg.collection_size;
    }
    return os << "}";
}

static void create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    auto set_clause = cfg.counters
        ? make_counter_set_clause(cfg.cell_count)
        : make_update_set_clause(cfg.cell_count, make_blob_hex(cfg.cell_size));
    if (cfg.collection_column && !cfg.counters) {
        set_clause += format(",\"CC\" = {}", make_set_literal(cfg.collection_size));
    }
    unsigned next_flush = (cfg.memtable_partitions > 0 ? cfg.memtable_partitions : cfg.partitions);
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        env.execute_cql(format("UPDATE cf SET {} WHERE \"KEY\"= 0x{};",
            set_clause, to_hex(make_key(sequence)))).get();
        if (sequence + 1 >= next_flush) {
            env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
            next_flush += cfg.memtable_partitions;
        }
    }

    if (cfg.flush_memtables) {
        std::cout << "Flushing partitions..." << std::endl;
        env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
    }
}

static int64_t make_random_seq(test_config& cfg) {
    return cfg.query_single_key ? 0 : tests::random::get_int<uint64_t>(cfg.partitions - 1);
}

static bytes make_random_key(test_config& cfg) {
    return make_key(make_random_seq(cfg));
}

static std::vector<perf_result> test_read(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    auto columns = make_column_list(cfg.cell_count);
    if (cfg.collection_column) {
        columns += ", \"CC\"";
    }
    sstring query = format("select {} from cf where \"KEY\" = ?", columns);
    if (cfg.bypass_cache) {
        query += " bypass cache";
    }
    if (!cfg.timeout.empty()) {
        query += " using timeout " + cfg.timeout;
    }
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_write(cql_test_env& env, test_config& cfg) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    auto blob_hex = make_blob_hex(cfg.cell_size);
    auto set_clause = make_update_set_clause(cfg.cell_count, blob_hex);
    if (cfg.collection_column) {
        set_clause += format(",\"CC\" = {}", make_set_literal(cfg.collection_size));
    }
    sstring query = format("UPDATE cf {}SET {} WHERE \"KEY\" = ?",
            usings, set_clause);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_delete(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    auto columns = make_column_list(cfg.cell_count);
    if (cfg.collection_column) {
        columns += ", \"CC\"";
    }
    sstring query = format("DELETE {} FROM cf {}WHERE \"KEY\" = ?",
            columns, usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_counter_update(cql_test_env& env, test_config& cfg) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring query = format("UPDATE cf {}SET {} WHERE \"KEY\" = ?",
            usings, make_counter_set_clause(cfg.cell_count));
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static schema_ptr make_counter_schema(std::string_view ks_name, unsigned cell_count) {
    auto builder = schema_builder(ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key);
    for (unsigned i = 0; i < cell_count; ++i) {
        builder.with_column(to_bytes(format("C{}", i)), counter_type);
    }
    return builder.build();
}

static std::vector<perf_result> do_cql_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name, cfg.cell_count);
        }
        auto builder = schema_builder(ks_name, "cf")
                .with_column("KEY", bytes_type, column_kind::partition_key);
        for (unsigned i = 0; i < cfg.cell_count; ++i) {
            builder.with_column(to_bytes(format("C{}", i)), bytes_type);
        }
        if (cfg.collection_column) {
            builder.with_column(to_bytes("CC"), set_type_impl::get_instance(int32_type, true));
        }
        return *builder.build();
    }).get();

    std::cout << "Disabling auto compaction" << std::endl;
    env.db().invoke_on_all([] (auto& db) {
        auto& cf = db.find_column_family("ks", "cf");
        return cf.disable_auto_compaction();
    }).get();

    switch (cfg.mode) {
    case test_config::run_mode::read:
        return test_read(env, cfg);
    case test_config::run_mode::write:
        if (cfg.counters) {
            return test_counter_update(env, cfg);
        } else {
            return test_write(env, cfg);
        }
    case test_config::run_mode::del:
        return test_delete(env, cfg);
    };
    abort();
}

void write_json_result(std::string result_file, const test_config& cfg, const aggregated_perf_results& agg) {
    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["partitions"] = cfg.partitions;
    params["cpus"] = smp::count;
    params["duration"] = cfg.duration_in_seconds;
    params["cell_count"] = cfg.cell_count;
    params["cell_size"] = cfg.cell_size;
    if (cfg.collection_column) {
        params["collection_column"] = true;
        params["collection_size"] = cfg.collection_size;
    }
    params["concurrency,partitions,cpus,duration"] = fmt::format("{},{},{},{}", cfg.concurrency, cfg.partitions, smp::count, cfg.duration_in_seconds);
    if (cfg.initial_tablets) {
        params["initial_tablets"] = cfg.initial_tablets.value();
    }

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }

    perf::write_json_result(result_file, agg, params, test_type);
}

/// If app configuration contains the named parameter, store its value into \p store.
static void set_from_cli(const char* name, app_template& app, utils::config_file::named_value<sstring>& store) {
    const auto& cfg = app.configuration();
    auto found = cfg.find(name);
    if (found != cfg.end()) {
        store(found->second.as<std::string>());
    }
}

namespace perf {

int scylla_simple_query_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("delete", "test delete path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test reading with a single key instead of random keys")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("counters", "test counters")
        ("tablets", "use tablets")
        ("initial-tablets", bpo::value<unsigned>()->default_value(128), "initial number of tablets")
        ("flush", "flush memtables before test")
        ("memtable-partitions", bpo::value<unsigned>(), "apply this number of partitions to memtable, then flush")
        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ("enable-cache", bpo::value<bool>()->default_value(true), "enable row cache")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("timeout", bpo::value<std::string>()->default_value(""), "use timeout")
        ("bypass-cache", "use bypass cache when querying")
        ("cell-count", bpo::value<unsigned>()->default_value(5), "number of cells (columns) per row")
        ("cell-size", bpo::value<unsigned>()->default_value(34), "size of each cell value in bytes")
        ("collection-column", "add a non-frozen set<int> collection column")
        ("collection-size", bpo::value<unsigned>()->default_value(10), "number of elements in the collection column")
        ("audit", bpo::value<std::string>(), "value for audit config entry")
        ("audit-keyspaces", bpo::value<std::string>(), "value for audit_keyspaces config entry")
        ("audit-tables", bpo::value<std::string>(), "value for audit_tables config entry")
        ("audit-categories", bpo::value<std::string>(), "value for audit_categories config entry")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';
        return smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        }).then([&app] () -> future<> {
            auto ext = std::make_shared<db::extensions>();
            ext->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
            auto db_cfg = ::make_shared<db::config>(ext);

            const auto enable_cache = app.configuration()["enable-cache"].as<bool>();
            std::cout << "enable-cache=" << enable_cache << '\n';
            db_cfg->enable_cache(enable_cache);
            cql_test_config cfg(db_cfg);
            if (app.configuration().contains("tablets")) {
                cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            set_from_cli("audit", app, cfg.db_config->audit);
            set_from_cli("audit-keyspaces", app, cfg.db_config->audit_keyspaces);
            set_from_cli("audit-tables", app, cfg.db_config->audit_tables);
            set_from_cli("audit-categories", app, cfg.db_config->audit_categories);
          return do_with_cql_env_thread([&app] (auto&& env) {
            auto cfg = test_config();
            cfg.partitions = app.configuration()["partitions"].as<unsigned>();
            cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg.query_single_key = app.configuration().contains("query-single-key");
            cfg.counters = app.configuration().contains("counters");
            cfg.flush_memtables = app.configuration().contains("flush");
            cfg.cell_count = app.configuration()["cell-count"].as<unsigned>();
            cfg.cell_size = app.configuration()["cell-size"].as<unsigned>();
            cfg.collection_column = app.configuration().contains("collection-column");
            cfg.collection_size = app.configuration()["collection-size"].as<unsigned>();
            if (app.configuration().contains("tablets")) {
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            if (app.configuration().contains("write")) {
                cfg.mode = test_config::run_mode::write;
            } else if (app.configuration().contains("delete")) {
                cfg.mode = test_config::run_mode::del;
            } else {
                cfg.mode = test_config::run_mode::read;
            };
            if (app.configuration().contains("operations-per-shard")) {
                cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            if (app.configuration().contains("memtable-partitions")) {
                cfg.memtable_partitions = app.configuration()["memtable-partitions"].as<unsigned>();
            }
            cfg.stop_on_error = app.configuration()["stop-on-error"].as<bool>();
            cfg.timeout = app.configuration()["timeout"].as<std::string>();
            cfg.bypass_cache = app.configuration().contains("bypass-cache");
            audit::audit::start_audit(env.local_db().get_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).handle_exception([&] (auto&& e) {
                fmt::print("audit start failed: {}", e);
            }).get();
            auto audit_stop = defer([] {
                audit::audit::stop_audit().get();
            });
            auto results = do_cql_test(env, cfg);
            aggregated_perf_results agg(results);
            std::cout << agg << std::endl;
            if (app.configuration().contains("json-result")) {
                write_json_result(app.configuration()["json-result"].as<std::string>(), cfg, agg);
            }
          }, std::move(cfg));
        });
    });
}

} // namespace perf
