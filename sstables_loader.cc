/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/units.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/sleep.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/short_streams.hh> 
#include "sstables_loader.hh"
#include "db/tablet_options.hh"
#include "db/config.hh"
#include "dht/auto_refreshing_sharder.hh"
#include "replica/distributed_loader.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstables.hh"
#include "gms/inet_address.hh"
#include "gms/feature_service.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "streaming/stream_reason.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "locator/abstract_replication_strategy.hh"
#include "message/messaging_service.hh"
#include "service/storage_service.hh"
#include "utils/error_injection.hh"
#include "sstables_loader_helpers.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "streaming/stream_blob.hh"
#include "db/view/view_update_checks.hh"
#include "db/view/view_builder.hh"
#include "db/view/view_building_worker.hh"
#include "cql3/untyped_result_set.hh"
#include "idl/sstables_loader.dist.hh"

#include "sstables/object_storage_client.hh"
#include "utils/rjson.hh"
#include "db/system_distributed_keyspace.hh"

#include <bit>
#include <cfloat>
#include <algorithm>

static logging::logger llog("sstables_loader");

namespace {

class send_meta_data {
    locator::host_id _node;
    seastar::rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> _sink;
    seastar::rpc::source<int32_t> _source;
    const bool _abort_supported = false;
    bool _error_from_peer = false;
    size_t _num_partitions_sent = 0;
    size_t _num_bytes_sent = 0;
    future<> _receive_done;
private:
    future<> do_receive() {
        int32_t status = 0;
        while (auto status_opt = co_await _source()) {
            status = std::get<0>(*status_opt);
            llog.debug("send_meta_data: got error code={}, from node={}", status, _node);
            if (status == -1) {
                _error_from_peer = true;
            }
        }
        llog.debug("send_meta_data: finished reading source from node={}", _node);
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error code={} from node={}", status, _node));
        }
        co_return;
    }
public:
    send_meta_data(locator::host_id node,
            seastar::rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> sink,
            seastar::rpc::source<int32_t> source, bool abort_supported)
        : _node(std::move(node))
        , _sink(std::move(sink))
        , _source(std::move(source))
        , _abort_supported(abort_supported)
        , _receive_done(make_ready_future<>()) {
    }
    void receive() {
        _receive_done = do_receive();
    }
    future<> send(const frozen_mutation_fragment& fmf, bool is_partition_start) {
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error from peer node={}", _node));
        }
        auto size = fmf.representation().size();
        if (is_partition_start) {
            ++_num_partitions_sent;
        }
        _num_bytes_sent += size;
        llog.trace("send_meta_data: send mf to node={}, size={}", _node, size);
        co_return co_await _sink(fmf, streaming::stream_mutation_fragments_cmd::mutation_fragment_data);
    }
    future<> finish(bool failed, bool aborted) {
        std::exception_ptr eptr;
        try {
            if (_abort_supported && aborted) {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::abort);
            } else if (failed) {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::error);
            } else {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::end_of_stream);
            }
        } catch (...) {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to send {} to node={}, err={}",
                    failed ? "stream_mutation_fragments_cmd::error" : "stream_mutation_fragments_cmd::end_of_stream", _node, eptr);
        }
        try {
            co_await _sink.close();
        } catch (...)  {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to close sink to node={}, err={}", _node, eptr);
        }
        try {
            co_await std::move(_receive_done);
        } catch (...)  {
            eptr = std::current_exception();
            llog.warn("send_meta_data: failed to process source from node={}, err={}", _node, eptr);
        }
        if (eptr) {
            std::rethrow_exception(eptr);
        }
        co_return;
    }
    size_t num_partitions_sent() {
        return _num_partitions_sent;
    }
    size_t num_bytes_sent() {
        return _num_bytes_sent;
    }
};

} // anonymous namespace

using primary_replica_only = bool_class<struct primary_replica_only_tag>;
using unlink_sstables = bool_class<struct unlink_sstables_tag>;
using defer_unlinking = bool_class<struct defer_unlinking_tag>;

class sstable_streamer {
protected:
    using stream_scope = sstables_loader::stream_scope;
    netw::messaging_service& _ms;
    replica::database& _db;
    replica::table& _table;
    locator::effective_replication_map_ptr _erm;
    std::vector<sstables::shared_sstable> _sstables;
    const primary_replica_only _primary_replica_only;
    const unlink_sstables _unlink_sstables;
    const stream_scope _stream_scope;
    // When set, every partition goes to exactly this host - the primary the scheduler assigned.
    std::optional<locator::host_id> _explicit_target;
    seastar::abort_source* _abort_source = nullptr;
public:
    void set_explicit_target(locator::host_id host) {
        _explicit_target = host;
    }

    void set_abort_source(seastar::abort_source& as) {
        _abort_source = &as;
    }

    sstable_streamer(netw::messaging_service& ms, replica::database& db, ::table_id table_id, locator::effective_replication_map_ptr erm,
                     std::vector<sstables::shared_sstable> sstables, primary_replica_only primary, unlink_sstables unlink, stream_scope scope)
            : _ms(ms)
            , _db(db)
            , _table(db.find_column_family(table_id))
            , _erm(std::move(erm))
            , _sstables(std::move(sstables))
            , _primary_replica_only(primary)
            , _unlink_sstables(unlink)
            , _stream_scope(scope)
    {
        // By sorting SSTables by their primary key, we allow SSTable runs to be
        // incrementally streamed.
        // Overlapping run fragments can have their content deduplicated, reducing
        // the amount of data we need to put on the wire.
        // Elements are popped off from the back of the vector, therefore we're sorting
        // it in descending order, to start from the smaller tokens.
        std::ranges::sort(_sstables, [] (const sstables::shared_sstable& x, const sstables::shared_sstable& y) {
            return x->compare_by_first_key(*y) > 0;
        });
    }

    virtual ~sstable_streamer() {}

    virtual future<> stream(shared_ptr<stream_progress> progress);
    host_id_vector_replica_set get_endpoints(const dht::token& token) const;
    future<> stream_sstable_mutations(streaming::plan_id, const dht::partition_range&, std::vector<sstables::shared_sstable>, defer_unlinking defer);
protected:
    virtual host_id_vector_replica_set get_primary_endpoints(const dht::token& token, std::function<bool(const locator::host_id&)> filter) const;
    future<> stream_sstables(const dht::partition_range&, std::vector<sstables::shared_sstable>, shared_ptr<stream_progress> progress, defer_unlinking defer);
private:
    host_id_vector_replica_set get_all_endpoints(const dht::token& token) const;
};

class tablet_sstable_streamer : public sstable_streamer {
    sharded<replica::database>& _db;
    const locator::tablet_map& _tablet_map;
public:
    tablet_sstable_streamer(netw::messaging_service& ms, sharded<replica::database>& db, ::table_id table_id, locator::effective_replication_map_ptr erm,
                            std::vector<sstables::shared_sstable> sstables, primary_replica_only primary, unlink_sstables unlink, stream_scope scope)
        : sstable_streamer(ms, db.local(), table_id, std::move(erm), std::move(sstables), primary, unlink, scope)
        , _db(db)
        , _tablet_map(_erm->get_token_metadata().tablets().get_tablet_map(table_id)) {
    }

    virtual future<> stream(shared_ptr<stream_progress> on_streamed) override;
    virtual host_id_vector_replica_set get_primary_endpoints(const dht::token& token, std::function<bool(const locator::host_id&)> filter) const override;

    future<> stream_one_tablet(const dht::token_range& tablet_range,
                               std::vector<sstables::shared_sstable> fully_contained,
                               std::vector<sstables::shared_sstable> partially_contained,
                               shared_ptr<stream_progress> progress);

private:
    host_id_vector_replica_set to_replica_set(const locator::tablet_replica_set& replicas) const {
        host_id_vector_replica_set result;
        result.reserve(replicas.size());
        for (auto&& replica : replicas) {
            result.push_back(replica.host);
        }
        return result;
    }

    using sst_classification_info = std::vector<std::vector<minimal_sst_info>>;

    future<> attach_sstable(shard_id from_shard, const sstring& ks, const sstring& cf, const minimal_sst_info& min_info) const {
        llog.debug("Adding downloaded SSTable gen={} to the table {}.{} on shard {}, submitted from shard {}", min_info.generation, _table.schema()->ks_name(), _table.schema()->cf_name(), this_shard_id(), from_shard);
        auto& db = _db.local();
        auto& table = db.find_column_family(ks, cf);
        auto& sst_manager = table.get_sstables_manager();
        auto sst = sst_manager.make_sstable(
            table.schema(), table.get_storage_options(), min_info.generation, sstables::sstable_state::normal, min_info.version, min_info.format);
        sst->set_sstable_level(0);
        auto units = co_await sst_manager.dir_semaphore().get_units(1);
        sstables::sstable_open_config cfg {
            .unsealed_sstable = true,
        };
        co_await sst->load(_erm->get_sharder(*table.schema()), cfg);
        co_await table.add_new_sstable_and_update_cache(sst, [&sst_manager, sst] (sstables::shared_sstable loading_sst) -> future<> {
            if (loading_sst == sst) {
                auto writer_cfg = sst_manager.configure_writer(loading_sst->get_origin());
                co_await loading_sst->seal_sstable(writer_cfg.backup);
            }
        });
    }

    future<>
    stream_fully_contained_sstables(const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables, shared_ptr<stream_progress> progress) {
        if (_stream_scope != stream_scope::node) {
            co_return co_await stream_sstables(pr, std::move(sstables), std::move(progress), defer_unlinking::no);
        }
        llog.debug("Directly downloading {} fully contained SSTables to local node from object storage.", sstables.size());
        auto downloaded_ssts = co_await download_fully_contained_sstables(std::move(sstables));

        co_await smp::invoke_on_all(
            [this, &downloaded_ssts, from = this_shard_id(), ks = _table.schema()->ks_name(), cf = _table.schema()->cf_name()] -> future<> {
                auto shard_ssts = std::move(downloaded_ssts[this_shard_id()]);
                for (const auto& min_info : shard_ssts) {
                    co_await attach_sstable(from, ks, cf, min_info);
                }
            });
        if (progress) {
            progress->advance(std::accumulate(downloaded_ssts.cbegin(), downloaded_ssts.cend(), 0., [](float acc, const auto& v) { return acc + v.size(); }));
        }
    }

    future<sst_classification_info> download_fully_contained_sstables(std::vector<sstables::shared_sstable> sstables) const {
        sst_classification_info downloaded_sstables(this_smp_shard_count());
        for (const auto& sstable : sstables) {
            // For now, tablet-aware restore doesn't need to mutate sstable level to 0
            // since we support only restoring to empty tables and so we can keep the sstable levels on backup.
            // Once we support restoring onto live tables we may want to mutate the ingested sstables' level to 0.
            auto min_info = co_await download_sstable(_db.local(), _table, sstable, llog);
            downloaded_sstables[min_info.shard].emplace_back(min_info);
        }
        co_return downloaded_sstables;
    }

    future<> unlink_marked_sstables() {
        co_await coroutine::parallel_for_each(_sstables, [] (sstables::shared_sstable& sst) -> future<> {
            if (sst->marked_for_deletion()) {
                co_await sst->unlink();
            }
        });
    }

    bool tablet_in_scope(locator::tablet_id) const;

    friend future<std::vector<tablet_sstable_collection>> get_sstables_for_tablets_for_tests(const std::vector<sstables::shared_sstable>& sstables,
                                                                                             std::vector<dht::token_range>&& tablets_ranges);
    // Pay attention, while working with tablet ranges, the `erm` must be held alive as long as we retrieve (and use here) tablet ranges from
    // the tablet map. This is already done when using `tablet_sstable_streamer` class but tread carefully if you plan to use this method somewhere else.
    static future<std::vector<tablet_sstable_collection>> get_sstables_for_tablets(const std::vector<sstables::shared_sstable>& sstables,
                                                                                   std::vector<dht::token_range>&& tablets_ranges);
};

host_id_vector_replica_set sstable_streamer::get_endpoints(const dht::token& token) const {
    if (_explicit_target) {
        return host_id_vector_replica_set{*_explicit_target};
    }
    auto host_filter = [&topo = _erm->get_topology(), scope = _stream_scope] (const locator::host_id& ep) {
        switch (scope) {
        case stream_scope::all:
            return true;
        case stream_scope::dc:
            return topo.get_datacenter(ep) == topo.get_datacenter();
        case stream_scope::rack:
            return topo.get_location(ep) == topo.get_location();
        case stream_scope::node:
            return topo.is_me(ep);
        }
    };

    if (_primary_replica_only) {
        if (_stream_scope == stream_scope::node) {
            throw std::runtime_error("Node scoped streaming of primary replica only is not supported");
        }
        return get_primary_endpoints(token, std::move(host_filter));
    }
    return get_all_endpoints(token) | std::views::filter(std::move(host_filter)) | std::ranges::to<host_id_vector_replica_set>();
}

host_id_vector_replica_set sstable_streamer::get_all_endpoints(const dht::token& token) const {
    auto current_targets = _erm->get_natural_replicas(token);
    auto pending = _erm->get_pending_replicas(token);
    std::move(pending.begin(), pending.end(), std::back_inserter(current_targets));
    return current_targets;
}

host_id_vector_replica_set sstable_streamer::get_primary_endpoints(const dht::token& token, std::function<bool(const locator::host_id&)> filter) const {
    auto current_targets = _erm->get_natural_replicas(token) | std::views::filter(std::move(filter)) | std::ranges::to<host_id_vector_replica_set>();
    current_targets.resize(1);
    return current_targets;
}

host_id_vector_replica_set tablet_sstable_streamer::get_primary_endpoints(const dht::token& token, std::function<bool(const locator::host_id&)> filter) const {
    auto tid = _tablet_map.get_tablet_id(token);
    auto replicas = locator::get_primary_replicas(_tablet_map, tid, _erm->get_topology(), [filter = std::move(filter)] (const locator::tablet_replica& replica) {
        return filter(replica.host);
    });
    return to_replica_set(replicas);
}

future<> sstable_streamer::stream(shared_ptr<stream_progress> progress) {
    if (progress) {
        progress->start(_sstables.size());
    }
    const auto full_partition_range = dht::partition_range::make_open_ended_both_sides();

    co_await stream_sstables(full_partition_range, std::move(_sstables), std::move(progress), defer_unlinking::no);
}

bool tablet_sstable_streamer::tablet_in_scope(locator::tablet_id tid) const {
    if (_stream_scope == stream_scope::all) {
        return true;
    }

    const auto& topo = _erm->get_topology();
    for (const auto& r : _tablet_map.get_tablet_info(tid).replicas) {
        switch (_stream_scope) {
        case stream_scope::node:
            if (topo.is_me(r.host)) {
                return true;
            }
            break;
        case stream_scope::rack:
            if (topo.get_location(r.host) == topo.get_location()) {
                return true;
            }
            break;
        case stream_scope::dc:
            if (topo.get_datacenter(r.host) == topo.get_datacenter()) {
                return true;
            }
            break;
        case stream_scope::all: // checked above already, but still need it here
            return true;
        }
    }
    return false;
}

// The tablet_sstable_streamer implements a hierarchical streaming strategy:
//
// 1. Top Level (Per-Tablet Streaming):
//    - Unlike vnode streaming, this streams sstables on a tablet-by-tablet basis
//    - For a table with M tablets, each tablet[i] maps to its own set of SSTable files
//      stored in tablet_to_sstables[i]
//    - If tablet_to_sstables[i] is empty, that tablet's streaming is considered complete
//    - Progress tracking advances by 1.0 unit when an entire tablet completes streaming
//
// 2. Inner Level (Per-SSTable Streaming):
//    - Within each tablet's batch, individual SSTables are streamed in smaller sub-batches
//    - The per_tablet_stream_progress class tracks streaming progress at this level:
//      - Updates when a set of SSTables completes streaming
//      - For n completed SSTables, advances by (n / total_sstables_in_current_tablet)
//      - Provides granular tracking for the inner level streaming operations
//      - Helps estimate completion time for the current tablet's batch
//
// Progress Tracking:
// The streaming progress is monitored at two granularity levels:
//    - Tablet level: Overall progress where each tablet contributes 1.0 units
//    - SSTable level: Progress of individual SSTable transfers within a tablet,
//                     managed by the per_tablet_stream_progress class
//
// Note: For simplicity, we assume uniform streaming time across tablets, even though
// tablets may vary significantly in their SSTable count or size. This assumption
// helps in progress estimation without requiring prior knowledge of SSTable
// distribution across tablets.
struct per_tablet_stream_progress : public stream_progress {
private:
    shared_ptr<stream_progress> _per_table_progress;
    const size_t _num_sstables_mapped;
public:
    per_tablet_stream_progress(shared_ptr<stream_progress> per_table_progress,
                               size_t num_sstables_mapped)
    : _per_table_progress(std::move(per_table_progress))
    , _num_sstables_mapped(num_sstables_mapped) {
        if (_per_table_progress && _num_sstables_mapped == 0) {
            // consider this tablet completed if nothing to stream
            _per_table_progress->advance(1.0);
        }
    }
    void advance(float num_sstable_streamed) override {
        // we should not move backward
        assert(num_sstable_streamed >= 0.);
        // we should call advance() only if the current tablet maps to at least
        // one sstable.
        assert(_num_sstables_mapped > 0);
        if (_per_table_progress) {
            _per_table_progress->advance(num_sstable_streamed / _num_sstables_mapped);
        }
    }
};

future<std::vector<tablet_sstable_collection>> tablet_sstable_streamer::get_sstables_for_tablets(const std::vector<sstables::shared_sstable>& sstables,
                                                                                                 std::vector<dht::token_range>&& tablets_ranges) {
    auto tablets_sstables =
        tablets_ranges | std::views::transform([](auto range) { return tablet_sstable_collection{.tablet_range = range}; }) | std::ranges::to<std::vector>();
    if (sstables.empty() || tablets_sstables.empty()) {
        co_return std::move(tablets_sstables);
    }
    // sstables are sorted by first key in reverse order.
    auto reversed_sstables = sstables | std::views::reverse;

    for (auto& [tablet_range, sstables_fully_contained, sstables_partially_contained] : tablets_sstables) {
        auto [fully, partially] = co_await get_sstables_for_tablet(reversed_sstables, tablet_range, [](const auto& sst) { return sst->get_first_decorated_key().token(); }, [](const auto& sst) { return sst->get_last_decorated_key().token(); });
        sstables_fully_contained = std::move(fully);
        sstables_partially_contained = std::move(partially);
    }
    co_return std::move(tablets_sstables);
}

future<> tablet_sstable_streamer::stream(shared_ptr<stream_progress> progress) {
    if (progress) {
        progress->start(_tablet_map.tablet_count());
    }

    auto classified_sstables = co_await get_sstables_for_tablets(
        _sstables, _tablet_map.tablet_ids() | std::views::filter([this](auto tid) { return tablet_in_scope(tid); }) | std::views::transform([this](auto tid) {
                       return _tablet_map.get_token_range(tid);
                   }) | std::ranges::to<std::vector>());

    for (auto& [tablet_range, sstables_fully_contained, sstables_partially_contained] : classified_sstables) {
        co_await stream_one_tablet(tablet_range, std::move(sstables_fully_contained),
                std::move(sstables_partially_contained), progress);
    }

    co_await unlink_marked_sstables();
}

future<> tablet_sstable_streamer::stream_one_tablet(const dht::token_range& tablet_range,
        std::vector<sstables::shared_sstable> fully_contained,
        std::vector<sstables::shared_sstable> partially_contained,
        shared_ptr<stream_progress> progress) {
    auto per_tablet_progress = make_shared<per_tablet_stream_progress>(
        progress,
        fully_contained.size() + partially_contained.size());
    auto tablet_pr = dht::to_partition_range(tablet_range);
    if (!partially_contained.empty()) {
        llog.debug("Streaming {} partially contained SSTables.", partially_contained.size());
        co_await stream_sstables(tablet_pr, std::move(partially_contained), per_tablet_progress, defer_unlinking::yes);
    }
    if (!fully_contained.empty()) {
        llog.debug("Streaming {} fully contained SSTables.", fully_contained.size());
        co_await stream_fully_contained_sstables(tablet_pr, std::move(fully_contained), per_tablet_progress);
    }
}

future<> sstable_streamer::stream_sstables(const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables, shared_ptr<stream_progress> progress, defer_unlinking defer) {
    size_t nr_sst_total = sstables.size();
    size_t nr_sst_current = 0;

    while (!sstables.empty()) {
        co_await utils::get_local_injector().inject("load_and_stream_before_streaming_batch",
            utils::wait_for_message(60s));

        const size_t batch_sst_nr = std::min(16uz, sstables.size());
        auto sst_processed = sstables
            | std::views::reverse
            | std::views::take(batch_sst_nr)
            | std::ranges::to<std::vector>();
        sstables.erase(sstables.end() - batch_sst_nr, sstables.end());

        auto ops_uuid = streaming::plan_id{utils::make_random_uuid()};
        llog.info("load_and_stream: started ops_uuid={}, process [{}-{}] out of {} sstables=[{}]",
            ops_uuid, nr_sst_current, nr_sst_current + sst_processed.size(), nr_sst_total,
            fmt::join(sst_processed | std::views::transform([] (auto sst) { return sst->get_filename(); }), ", "));
        nr_sst_current += sst_processed.size();
        co_await stream_sstable_mutations(ops_uuid, pr, std::move(sst_processed), defer);
        if (progress) {
            progress->advance(batch_sst_nr);
        }
    }
}

future<> sstable_streamer::stream_sstable_mutations(streaming::plan_id ops_uuid, const dht::partition_range& pr, std::vector<sstables::shared_sstable> sstables, defer_unlinking defer) {
    const auto token_range = pr.transform(std::mem_fn(&dht::ring_position::token));
    auto s = _table.schema();
    const auto cf_id = s->id();
    const auto reason = streaming::stream_reason::repair;

    auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, std::move(token_range)));
    size_t estimated_partitions = 0;
    for (auto& sst : sstables) {
        estimated_partitions += co_await sst->estimated_keys_for_range(token_range);
        sst_set->insert(sst);
    }

    auto start_time = std::chrono::steady_clock::now();
    host_id_vector_replica_set current_targets;
    std::unordered_map<locator::host_id, send_meta_data> metas;
    size_t num_partitions_processed = 0;
    size_t num_bytes_read = 0;
    auto permit = co_await _db.obtain_reader_permit(_table, "sstables_loader::load_and_stream()", db::no_timeout, {});
    auto reader = mutation_fragment_v1_stream(_table.make_streaming_reader(s, std::move(permit), pr, sst_set, gc_clock::now()));
    std::exception_ptr eptr;
    bool failed = false;

    try {
        while (auto mf = co_await reader()) {
            if (_abort_source) {
                _abort_source->check();
            }
            bool is_partition_start = mf->is_partition_start();
            if (is_partition_start) {
                ++num_partitions_processed;
                auto& start = mf->as_partition_start();
                const auto& current_dk = start.key();

                current_targets = get_endpoints(current_dk.token());
                llog.trace("load_and_stream: ops_uuid={}, current_dk={}, current_targets={}", ops_uuid,
                        current_dk.token(), current_targets);
                for (auto& node : current_targets) {
                    if (!metas.contains(node)) {
                        auto [sink, source] = co_await _ms.make_sink_and_source_for_stream_mutation_fragments(reader.schema()->version(),
                                ops_uuid, cf_id, estimated_partitions, reason, service::default_session_id, node);
                        bool abort_supported = _ms.supports_load_and_stream_abort_rpc_message();
                        llog.debug("load_and_stream: ops_uuid={}, make sink and source for node={}", ops_uuid, node);
                        metas.emplace(node, send_meta_data(node, std::move(sink), std::move(source), abort_supported));
                        metas.at(node).receive();
                    }
                }
            }
            frozen_mutation_fragment fmf = freeze(*s, *mf);
            num_bytes_read += fmf.representation().size();
            co_await coroutine::parallel_for_each(current_targets, [&metas, &fmf, is_partition_start] (const locator::host_id& node) {
                return metas.at(node).send(fmf, is_partition_start);
            });
        }
    } catch (...) {
        failed = true;
        eptr = std::current_exception();
        llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, send_phase, err={}",
                ops_uuid, s->ks_name(), s->cf_name(), eptr);
    }
    co_await reader.close();
    try {
        co_await coroutine::parallel_for_each(metas.begin(), metas.end(), [failed, eptr] (std::pair<const locator::host_id, send_meta_data>& pair) {
            auto& meta = pair.second;
            if (eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const abort_requested_exception&) {
                    return meta.finish(failed, true);
                } catch (...) {
                    // just fall through
                }
            }
            return meta.finish(failed, false);
        });
    } catch (...) {
        failed = true;
        eptr = std::current_exception();
        llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, finish_phase, err={}",
                ops_uuid, s->ks_name(), s->cf_name(), eptr);
    }
    if (!failed && _unlink_sstables) {
        try {
            co_await coroutine::parallel_for_each(sstables, [&] (sstables::shared_sstable& sst) {
                llog.debug("load_and_stream: ops_uuid={}, ks={}, table={}, remove sst={}",
                        ops_uuid, s->ks_name(), s->cf_name(), sst->toc_filename());
                if (defer) {
                    sst->mark_for_deletion();
                    return make_ready_future<>();
                } else {
                    return sst->unlink();
                }
            });
        } catch (...) {
            failed = true;
            eptr = std::current_exception();
            llog.warn("load_and_stream: ops_uuid={}, ks={}, table={}, del_sst_phase, err={}",
                    ops_uuid, s->ks_name(), s->cf_name(), eptr);
        }
    }
    auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(std::chrono::steady_clock::now() - start_time).count();
    for (auto& [node, meta] : metas) {
        llog.info("load_and_stream: ops_uuid={}, ks={}, table={}, target_node={}, num_partitions_sent={}, num_bytes_sent={}",
                ops_uuid, s->ks_name(), s->cf_name(), node, meta.num_partitions_sent(), meta.num_bytes_sent());
    }
    auto partition_rate = std::fabs(duration) > FLT_EPSILON ? num_partitions_processed / duration : 0;
    auto bytes_rate = std::fabs(duration) > FLT_EPSILON ? num_bytes_read / duration / 1024 / 1024 : 0;
    auto status = failed ? "failed" : "succeeded";
    llog.info("load_and_stream: finished ops_uuid={}, ks={}, table={}, partitions_processed={} partitions, bytes_processed={} bytes, partitions_per_second={} partitions/s, bytes_per_second={} MiB/s, duration={} s, status={}",
            ops_uuid, s->ks_name(), s->cf_name(), num_partitions_processed, num_bytes_read, partition_rate, bytes_rate, duration, status);
    if (failed) {
        std::rethrow_exception(eptr);
    }
}

future<locator::effective_replication_map_ptr> sstables_loader::await_topology_quiesced_and_get_erm(::table_id table_id) {
    // By waiting for topology to quiesce, we guarantee load-and-stream will not start in the middle
    // of a topology operation that changes the token range boundaries, e.g. split or merge.
    // Split, for example, first executes the barrier and then splits the tablets.
    // So it can happen a sstable is generated between those steps and will incorrectly span two
    // tablets. We want to serialize load-and-stream and split finalization (a topology op).

    locator::effective_replication_map_ptr erm;
    while (true) {
        auto& t = _db.local().find_column_family(table_id);
        erm = t.get_effective_replication_map();
        auto expected_topology_version = erm->get_token_metadata().get_version();
        auto& ss = _ss.local();

        // The awaiting only works with raft enabled, and we only need it with tablets,
        // so let's bypass the awaiting when tablet is disabled.
        if (!t.uses_tablets()) {
            break;
        }
        // optimistically attempt to grab an erm on quiesced topology
        if (co_await ss.verify_topology_quiesced(expected_topology_version)) {
            break;
        }
        erm = nullptr;
        co_await _ss.local().await_topology_quiesced();
    }

    co_return std::move(erm);
}

future<> sstables_loader::load_and_stream(sstring ks_name, sstring cf_name,
        ::table_id table_id, std::vector<sstables::shared_sstable> sstables, primary_replica_only primary, bool unlink, stream_scope scope,
        shared_ptr<stream_progress> progress) {
    // streamer guarantees topology stability, for correctness, by holding effective_replication_map
    // throughout its lifetime.
    auto erm = co_await await_topology_quiesced_and_get_erm(table_id);

    // Obtain a phaser guard to prevent the table from being destroyed
    // while streaming is in progress.  table::stop() calls
    // _pending_streams_phaser.close() which blocks until all outstanding
    // stream_in_progress() guards are released, so holding this guard
    // keeps the table alive for the entire streaming operation.
    // find_column_family throws no_such_column_family if the table was
    // already dropped before we got here.
    auto& tbl = _db.local().find_column_family(table_id);
    auto stream_guard = tbl.stream_in_progress();

    std::unique_ptr<sstable_streamer> streamer;
    if (tbl.uses_tablets()) {
        streamer =
            std::make_unique<tablet_sstable_streamer>(_messaging, _db, table_id, std::move(erm), std::move(sstables), primary, unlink_sstables(unlink), scope);
    } else {
        streamer =
            std::make_unique<sstable_streamer>(_messaging, _db.local(), table_id, std::move(erm), std::move(sstables), primary, unlink_sstables(unlink), scope);
    }

    co_await streamer->stream(progress);
}

// For more details, see distributed_loader::process_upload_dir().
// All the global operations are going to happen here, and just the reloading happens
// in there.
future<> sstables_loader::load_new_sstables(sstring ks_name, sstring cf_name,
    bool load_and_stream, bool primary, bool skip_cleanup, bool skip_reshape, stream_scope scope) {
    if (_loading_new_sstables) {
        throw std::runtime_error("Already loading SSTables. Try again later");
    } else {
        _loading_new_sstables = true;
    }

    co_await coroutine::switch_to(_sched_group);

    sstring load_and_stream_desc = fmt::format("{}", load_and_stream);
    const auto& rs = _db.local().find_keyspace(ks_name).get_replication_strategy();
    if (rs.is_per_table() && !load_and_stream) {
        load_and_stream = true;
        load_and_stream_desc = "auto-enabled-for-tablets";
    }

    if (load_and_stream && skip_cleanup) {
        throw std::runtime_error("Skipping cleanup is not possible when doing load-and-stream");
    }

    if (load_and_stream && skip_reshape) {
        throw std::runtime_error("Skipping reshape is not possible when doing load-and-stream");
    }

    llog.info("Loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, skip_cleanup={}, skip_reshape={}, scope={}",
            ks_name, cf_name, load_and_stream_desc, primary, skip_cleanup, skip_reshape, scope);

    try {
        if (load_and_stream) {
            ::table_id table_id;
            std::vector<std::vector<sstables::shared_sstable>> sstables_on_shards;
            // Load-and-stream reads the entire content from SSTables, therefore it can afford to discard the bloom filter
            // that might otherwise consume a significant amount of memory.
            sstables::sstable_open_config cfg {
                .load_bloom_filter = false,
            };
            std::tie(table_id, sstables_on_shards) = co_await replica::distributed_loader::get_sstables_from_upload_dir(_db, ks_name, cf_name, cfg);
            co_await container().invoke_on_all([&sstables_on_shards, ks_name, cf_name, table_id, primary, scope] (sstables_loader& loader) mutable -> future<> {
                co_await loader.load_and_stream(ks_name, cf_name, table_id, std::move(sstables_on_shards[this_shard_id()]), primary_replica_only(primary), true, scope, {});
            });
        } else {
            co_await replica::distributed_loader::process_upload_dir(_db, _view_builder, _view_building_worker, ks_name, cf_name, skip_cleanup, skip_reshape);
        }
    } catch (...) {
        llog.warn("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=failed: {}",
                ks_name, cf_name, load_and_stream, primary, std::current_exception());
        _loading_new_sstables = false;
        throw;
    }
    llog.info("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=succeeded",
            ks_name, cf_name, load_and_stream, primary);
    _loading_new_sstables = false;
    co_return;
}

class sstables_loader::download_task_impl : public tasks::task_manager::task::impl {
    sharded<sstables_loader>& _loader;
    sstring _endpoint;
    sstring _bucket;
    sstring _ks;
    sstring _cf;
    sstring _prefix;
    sstables_loader::stream_scope _scope;
    std::vector<sstring> _sstables;
    const primary_replica_only _primary_replica;
    struct progress_holder {
        // Wrap stream_progress in a smart pointer to enable polymorphism.
        // This allows derived progress types to be passed down for per-tablet
        // progress tracking while maintaining the base interface.
        shared_ptr<stream_progress> progress = make_shared<stream_progress>();
    };
    mutable shared_mutex _progress_mutex;
    // user could query for the progress even before _progress_per_shard
    // is completed started, and this._status.state does not reflect the
    // state of progress, so we have to track it separately.
    enum class progress_state {
        uninitialized,
        initialized,
        finalized,
    } _progress_state = progress_state::uninitialized;
    sharded<progress_holder> _progress_per_shard;
    tasks::task_manager::task::progress _final_progress;

protected:
    virtual future<> run() override;

public:
    download_task_impl(tasks::task_manager::module_ptr module, sharded<sstables_loader>& loader,
            sstring endpoint, sstring bucket, sstring ks, sstring cf, sstring prefix, std::vector<sstring> sstables,
            sstables_loader::stream_scope scope, primary_replica_only primary_replica) noexcept
        : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
        , _loader(loader)
        , _endpoint(std::move(endpoint))
        , _bucket(std::move(bucket))
        , _ks(std::move(ks))
        , _cf(std::move(cf))
        , _prefix(std::move(prefix))
        , _scope(scope)
        , _sstables(std::move(sstables))
        , _primary_replica(primary_replica)
    {
        _status.progress_units = "batches";
    }

    virtual std::string type() const override {
        return "download_sstables";
    }

    virtual tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::no;
    }

    virtual tasks::is_user_task is_user_task() const noexcept override {
        return tasks::is_user_task::yes;
    }

    tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable::yes;
    }

    virtual future<> release_resources() noexcept override {
        // preserve the final progress, so we can access it after the task is
        // finished
        _final_progress = co_await get_progress();
        co_await with_lock(_progress_mutex, [this] -> future<> {
            if (std::exchange(_progress_state, progress_state::finalized) == progress_state::initialized) {
                co_await _progress_per_shard.stop();
            }
        });
    }

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        co_return co_await with_shared(_progress_mutex, [this] -> future<tasks::task_manager::task::progress> {
            switch (_progress_state) {
            case progress_state::uninitialized:
                co_return tasks::task_manager::task::progress{};
            case progress_state::finalized:
                co_return _final_progress;
            case progress_state::initialized:
                break;
            }
            auto p = co_await _progress_per_shard.map_reduce(
                adder<stream_progress>{},
                [] (const progress_holder& holder) -> stream_progress {
                  auto p = holder.progress;
                  SCYLLA_ASSERT(p);
                  return *p;
                });
            co_return tasks::task_manager::task::progress {
                .completed = p.completed,
                .total = p.total,
            };
        });
    }
};

future<> sstables_loader::download_task_impl::run() {
    // Load-and-stream reads the entire content from SSTables, therefore it can afford to discard the bloom filter
    // that might otherwise consume a significant amount of memory.
    sstables::sstable_open_config cfg {
        .load_bloom_filter = false,
    };
    llog.debug("Loading sstables from {}({}/{})", _endpoint, _bucket, _prefix);

    auto ep_type = _loader.local()._storage_manager.get_endpoint_type(_endpoint);
    std::vector<seastar::abort_source> shard_aborts(this_smp_shard_count());
    auto [ table_id, sstables_on_shards ] = co_await replica::distributed_loader::get_sstables_from_object_store(_loader.local()._db, _ks, _cf, _sstables, _endpoint, ep_type, _bucket, _prefix, cfg, [&] {
        return &shard_aborts[this_shard_id()];
    });
    llog.debug("Streaming sstables from {}({}/{})", _endpoint, _bucket, _prefix);
    std::exception_ptr ex;
    named_gate g("sstables_loader::download_task_impl");
    try {
        _as.check();

        auto s = _as.subscribe([&]() noexcept {
            try {
                auto h = g.hold();
                (void)smp::invoke_on_all([&shard_aborts, ex = _as.abort_requested_exception_ptr()] {
                    shard_aborts[this_shard_id()].request_abort_ex(ex);
                }).finally([h = std::move(h)] {});
            } catch (...) {
            }
        });
        co_await _progress_per_shard.start();
        _progress_state = progress_state::initialized;
        co_await _loader.invoke_on_all([this, &sstables_on_shards, table_id] (sstables_loader& loader) mutable -> future<> {
            co_await loader.load_and_stream(_ks, _cf, table_id, std::move(sstables_on_shards[this_shard_id()]), _primary_replica, false, _scope,
                                            _progress_per_shard.local().progress);
        });
    } catch (...) {
        ex = std::current_exception();
    }

    co_await g.close();

    if (_as.abort_requested()) {
        if (!ex) {
            ex = _as.abort_requested_exception_ptr();
        }
    }

    if (ex) {
        co_await _loader.invoke_on_all([&sstables_on_shards] (sstables_loader&) {
            sstables_on_shards[this_shard_id()] = {}; // clear on correct shard
        });
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

sstables_loader::sstables_loader(sharded<replica::database>& db,
        sharded<service::storage_service>& ss,
        netw::messaging_service& messaging,
        sharded<db::view::view_builder>& vb,
        sharded<db::view::view_building_worker>& vbw,
        tasks::task_manager& tm,
        sstables::storage_manager& sstm,
        db::system_distributed_keyspace& sys_dist_ks,
        seastar::scheduling_group sg)
    : _db(db)
    , _ss(ss)
    , _messaging(messaging)
    , _view_builder(vb)
    , _view_building_worker(vbw)
    , _task_manager_module(make_shared<task_manager_module>(tm))
    , _storage_manager(sstm)
    , _sys_dist_ks(sys_dist_ks)
    , _sched_group(std::move(sg))
{
    tm.register_module("sstables_loader", _task_manager_module);
    ser::sstables_loader_rpc_verbs::register_restore_tablet(&_messaging, [this] (raft::server_id dst_id, locator::global_tablet_id gid) -> future<restore_result> {
        return _ss.local().handle_raft_rpc(dst_id, [&sl = container(), gid] (auto& ss) {
            return ss.do_tablet_operation(gid, "Restore", [&sl, gid] (locator::tablet_metadata_guard& guard) -> future<service::tablet_operation_result> {
                co_await sl.local().download_tablet_sstables(gid, guard);
                co_return service::tablet_operation_empty_result{};
            }).then([] (auto res) {
                return make_ready_future<restore_result>();
            });
        });
    });
    ser::sstables_loader_rpc_verbs::register_prepare_upload(&_messaging, [this] (rpc::opt_time_point, raft::server_id dst_id, ::table_id table) -> future<prepare_upload_response> {
        return _ss.local().handle_raft_rpc(dst_id, [&sl = container(), table] (auto&) {
            return sl.local().prepare_upload(table);
        });
    });
    // stream_blob_handler() looks ops_id up in a node-local map only the initiator populates, so
    // migration has the destination start the transfer. Cluster upload cannot - only the sender
    // knows what is in its upload directory - so it asks the receiver to register the id first.
    ser::sstables_loader_rpc_verbs::register_upload_stream_session(&_messaging, [this] (raft::server_id dst_id, utils::UUID ops_id, bool start) -> future<upload_stream_session_result> {
        return _ss.local().handle_raft_rpc(dst_id, [ops_id, start] (auto&) -> future<> {
            auto id = streaming::file_stream_id(ops_id);
            if (start) {
                co_await streaming::mark_tablet_stream_start(id);
            } else {
                co_await streaming::mark_tablet_stream_done(id);
            }
        }).then([] {
            return make_ready_future<upload_stream_session_result>();
        });
    });
    ser::sstables_loader_rpc_verbs::register_upload_replicate_tablet(&_messaging, [this] (raft::server_id dst_id, locator::global_tablet_id gid, uint32_t dst_shard) -> future<upload_replicate_result> {
        return _ss.local().handle_raft_rpc(dst_id, [&sl = container(), gid, dst_shard] (auto&) {
            return sl.local().do_upload_replicate_tablet(gid, dst_shard);
        });
    });
    ser::sstables_loader_rpc_verbs::register_finish_upload(&_messaging, [this] (rpc::opt_time_point, raft::server_id dst_id, utils::UUID request_id, bool unlink_consumed) -> future<finish_upload_result> {
        return _ss.local().handle_raft_rpc(dst_id, [&sl = container(), request_id, unlink_consumed] (auto&) {
            return sl.local().finish_upload(request_id, unlink_consumed);
        }).then([] {
            return make_ready_future<finish_upload_result>();
        });
    });
    ser::sstables_loader_rpc_verbs::register_upload_tablet(&_messaging, [this] (raft::server_id dst_id, locator::global_tablet_id gid, std::vector<uint32_t> source_shards) -> future<upload_tablet_result> {
        return _ss.local().handle_raft_rpc(dst_id, [&sl = container(), gid, source_shards = std::move(source_shards)] (auto&) mutable {
            return sl.local().do_upload_tablet(gid, std::move(source_shards));
        });
    });
}

future<> sstables_loader::stop() {
    co_await ser::sstables_loader_rpc_verbs::unregister(&_messaging),
    co_await _task_manager_module->stop();
    _upload_sessions.clear();
}

// Where an uploaded sstable of this table lands: past the view-update path it is a normal
// sstable, otherwise it goes to staging so view building sees it.
future<sstables::sstable_state> sstables_loader::upload_destination_state(replica::table& tbl) {
    auto decision = co_await db::view::check_needs_view_update_path(_view_builder.local(),
            _db.local().get_token_metadata_ptr(), tbl, streaming::stream_reason::repair);
    co_return decision == db::view::sstable_destination_decision::normal_directory
            ? sstables::sstable_state::normal : sstables::sstable_state::staging;
}

future<> sstables_loader::ensure_upload_session(utils::UUID request_id, ::table_id table_id) {
    if (this_shard_id() != 0) {
        co_return co_await container().invoke_on(0, [request_id, table_id] (sstables_loader& loader) {
            return loader.ensure_upload_session(request_id, table_id);
        });
    }

    auto units = co_await get_units(_prepare_upload_sem, 1);
    if (_upload_sessions.contains(request_id)) {
        co_return;
    }
    if (_draining_upload_sessions.contains(request_id)) {
        // finish_upload() unpublished this request's sessions and is tearing them down; it released
        // the semaphore between those steps, which is what let this call in. Opening now would
        // publish a session nothing will ever tear down.
        throw std::runtime_error(fmt::format(
                "Upload request {} is being torn down", request_id));
    }
    // A transition created just before its request was retired can arrive after FINISH_UPLOAD
    // has torn the sessions down. Opening one for it would ingest a slice of a retired load
    // into a session nothing tears down. The coordinator barriers before it sends, so the
    // applied group0 state here is current enough to say whether the request still runs.
    if (!_ss.local().is_upload_request_ongoing(request_id)) {
        throw std::runtime_error(fmt::format(
                "Upload request {} is no longer ongoing", request_id));
    }

    auto& tbl = _db.local().find_column_family(table_id);
    auto s = tbl.schema();
    llog.info("Opening upload directory of {}.{} for upload request {}", s->ks_name(), s->cf_name(), request_id);

    sstables::sstable_open_config cfg {
        .load_bloom_filter = false,
    };
    auto [tid, sstables_on_shards] = co_await replica::distributed_loader::get_sstables_from_upload_dir(
            _db, s->ks_name(), s->cf_name(), cfg);

    co_await container().invoke_on_all([request_id, table_id, &sstables_on_shards] (sstables_loader& loader) {
        auto session = make_lw_shared<upload_session>();
        session->table = table_id;
        session->sstables = std::move(sstables_on_shards[this_shard_id()]);
        std::ranges::sort(session->sstables, [] (const sstables::shared_sstable& x, const sstables::shared_sstable& y) {
            return x->compare_by_first_key(*y) < 0;
        });
        loader._upload_sessions.emplace(request_id, std::move(session));
    });
}

future<> sstables_loader::finish_upload(utils::UUID request_id, bool unlink_consumed) {
    if (this_shard_id() != 0) {
        co_return co_await container().invoke_on(0, [request_id, unlink_consumed] (sstables_loader& loader) {
            return loader.finish_upload(request_id, unlink_consumed);
        });
    }

    // Unpublish under the semaphore, tear down outside it.
    //
    // Unpublishing races ensure_upload_session(), which holds the semaphore while it scans: without
    // it a FINISH_UPLOAD arriving mid-scan finds no session to tear down, and the scan then
    // publishes sessions nothing will ever ask about again. The teardown must not hold it: it
    // waits on attach_gate, and prepare_upload() takes the same semaphore from a verb the
    // coordinator awaits.
    {
        auto units = co_await get_units(_prepare_upload_sem, 1);
        co_await container().invoke_on_all([request_id] (sstables_loader& loader) {
            auto it = loader._upload_sessions.find(request_id);
            if (it == loader._upload_sessions.end()) {
                return;
            }
            loader._draining_upload_sessions.insert_or_assign(request_id, std::move(it->second));
            loader._upload_sessions.erase(it);
        });
    }

    co_await container().invoke_on_all([request_id, unlink_consumed] (sstables_loader& loader) -> future<> {
        auto it = loader._draining_upload_sessions.find(request_id);
        if (it == loader._draining_upload_sessions.end()) {
            co_return;
        }
        // The entry stays, emptied, until this teardown is done. Erasing it up front made the
        // guard in ensure_upload_session() dead: this lambda runs on shard 0 synchronously up to
        // its first suspension, so a waiter let in by the semaphore's release above found neither
        // map holding the request and rescanned the directory for a request being torn down.
        // Emptied rather than kept whole so a second FINISH_UPLOAD does not tear down twice.
        auto session = std::exchange(it->second, nullptr);
        if (!session) {
            co_return;
        }
        auto forget = seastar::defer([&loader, request_id] () noexcept {
            loader._draining_upload_sessions.erase(request_id);
        });

        // The session is out of the published map and the gate waits for an attach already
        // running. Only past both does pending_attach mean what the branch below needs - a file
        // moved out of the upload directory and in no sstable set.
        co_await session->attach_gate.close();

        if (!unlink_consumed) {
            // Aborted or failed: leave what is still in the upload directory for the operator.
            // What an attach already moved out is different - it sits in the table's directory in no
            // sstable set, marked consumed so no rescan finds it, and would be adopted on the next
            // restart, resurrecting a slice of an aborted load.
            size_t removed = 0;
            for (auto& entry : session->pending_attach) {
                for (auto& m : entry.second) {
                    try {
                        removed += co_await loader.container().invoke_on(m.owning_shard,
                                [table = session->table, m] (sstables_loader& owner) -> future<size_t> {
                            auto& tbl = owner._db.local().find_column_family(table);
                            // With attach_gate closed this cannot be in the sstable set, and
                            // unlinking it if it were would destroy live data.
                            auto all_sstables = tbl.get_sstables();
                            for (const auto& sst : *all_sstables) {
                                if (sst->generation() == m.generation) {
                                    llog.error("Refusing to remove upload sstable (generation {}) of table "
                                            "{}: it is attached to the table, which a torn-down pending "
                                            "attach must never be", m.generation, table);
                                    co_return 0;
                                }
                            }
                            auto erm = tbl.get_effective_replication_map();
                            auto sst = tbl.get_sstables_manager().make_sstable(tbl.schema(),
                                    tbl.get_storage_options(), m.generation, m.state, m.version, m.format);
                            co_await sst->load(erm->get_sharder(*tbl.schema()));
                            co_await sst->unlink();
                            co_return 1;
                        });
                    } catch (...) {
                        llog.warn("Failed to remove orphaned upload sstable (generation {}) of table {}: {}",
                                m.generation, session->table, std::current_exception());
                    }
                }
            }
            llog.info("Upload request {} torn down on shard {}, {} sstables left in the upload "
                    "directory, {} orphaned attach(es) removed",
                    request_id, this_shard_id(), session->sstables.size(), removed);
            co_return;
        }

        size_t marked = 0;
        for (auto& sst : session->sstables) {
            if (session->streamed.contains(sst.get()) && !session->unlinked.contains(sst.get())) {
                marked++;
                sst->mark_for_deletion();
                session->unlinked.insert(sst.get());
            }
        }
        llog.info("Upload request {} finished on shard {}, marked {} of {} sstables for deletion",
                request_id, this_shard_id(), marked, session->sstables.size());
    });
}

// Phase 2: pull the tablet's range from the primary by file streaming - the mechanism tablet
// migration uses, driven from the destination. Sstable state is carried across, so one still in
// staging on the primary arrives in staging here and this replica registers its own view building
// work. That is required: view updates are generated per base replica.
future<size_t> sstables_loader::attach_local_upload_sstables(locator::global_tablet_id gid,
        shard_id owning_shard, std::vector<sstables::shared_sstable>& fully_contained,
        upload_session& session) {
    // Not just fully_contained: a previous attempt may have moved sstables out without finishing
    // the attach, and those are marked consumed, so classification no longer offers them.
    auto pending_it = session.pending_attach.find(gid.tablet.value());
    bool has_pending = pending_it != session.pending_attach.end() && !pending_it->second.empty();
    auto register_it = session.pending_register.find(gid.tablet.value());
    bool has_register = register_it != session.pending_register.end() && !register_it->second.empty();
    if (fully_contained.empty() && !has_pending && !has_register) {
        co_return 0;
    }
    // Held from before the first file leaves the upload directory until the attach is recorded.
    // finish_upload() closes this gate before reading pending_attach, so it cannot observe a
    // half-done attach or unlink a file already in the table.
    auto attach_hold = session.attach_gate.hold();
    auto& tbl = _db.local().find_column_family(gid.table);

    auto state = co_await upload_destination_state(tbl);

    auto& pending = session.pending_attach[gid.tablet.value()];

    // Move each file out under a fresh generation - nothing is read or re-encoded. Version and
    // format travel with it: they vary per sstable, and reopening with the wrong ones does not
    // describe the file on disk.
    for (auto& sst : fully_contained) {
        auto gen = tbl.get_sstable_generation_generator()();
        auto version = sst->get_version();
        auto format = sst->get_format();
        co_await sst->pick_up_from_upload(state, gen);
        session.consumed.insert(sst.get());
        pending.push_back(upload_session::moved_sstable{gen, version, format, state, owning_shard});
    }
    auto taken = fully_contained.size();
    fully_contained.clear();

    // Attached on the shard owning the tablet, which need not be the shard that opened them.
    // attached counts how many of pending are in the table: add_sstables_and_update_cache() is a
    // loop, so re-adding on a retry would give the table two sstables over the same files.
    size_t attached = 0;
    std::exception_ptr ex;
    try {
        co_await container().invoke_on(owning_shard, [gid, &pending, &attached]
                (sstables_loader& loader) -> future<> {
            auto& tbl = loader._db.local().find_column_family(gid.table);
            auto& sst_manager = tbl.get_sstables_manager();
            auto erm = tbl.get_effective_replication_map();
            for (size_t i = 0; i < pending.size(); i++) {
                if (i > 0) {
                    utils::get_local_injector().inject("upload_attach_fail_once", [] {
                        throw std::runtime_error("upload_attach_fail_once");
                    });
                }
                const auto& m = pending[i];
                auto sst = sst_manager.make_sstable(tbl.schema(), tbl.get_storage_options(), m.generation,
                        m.state, m.version, m.format);
                co_await sst->load(erm->get_sharder(*tbl.schema()));
                co_await tbl.add_sstables_and_update_cache({sst});
                attached = i + 1;
            }
            llog.debug("Attached {} upload sstables directly for tablet {} on owning shard {}",
                    pending.size(), gid, this_shard_id());
        });
    } catch (...) {
        ex = std::current_exception();
    }
    // Record them as owing registration before dropping them from the attach list: erasing the
    // attach record with it still owed would report success with no view built.
    {
        std::vector<upload_session::moved_sstable> newly_staged;
        for (size_t i = 0; i < attached; i++) {
            if (pending[i].state == sstables::sstable_state::staging) {
                newly_staged.push_back(pending[i]);
            }
        }
        if (!newly_staged.empty()) {
            auto& owed = session.pending_register[gid.tablet.value()];
            owed.insert(owed.end(), newly_staged.begin(), newly_staged.end());
        }
    }
    pending.erase(pending.begin(), pending.begin() + attached);
    auto remaining = pending.size();
    if (remaining == 0) {
        session.pending_attach.erase(gid.tablet.value());
    }
    if (ex) {
        llog.warn("Attaching upload sstables for tablet {} on shard {} failed after {} of them were "
                "attached, {} left for the retry: {}", gid, this_shard_id(), attached, remaining, ex);
        co_await coroutine::return_exception_ptr(std::move(ex));
    }

    // Re-found by generation, since the objects that attached them are gone.
    // register_staging_sstable_tasks() only queues, and queues nothing if it threw, so re-running
    // neither double-registers nor loses any.
    if (auto owed_it = session.pending_register.find(gid.tablet.value());
            owed_it != session.pending_register.end() && !owed_it->second.empty()) {
        co_await container().invoke_on(owning_shard, [gid, owed = owed_it->second]
                (sstables_loader& loader) -> future<> {
            auto& tbl = loader._db.local().find_column_family(gid.table);
            auto all_sstables = tbl.get_sstables();
            std::vector<sstables::shared_sstable> staged;
            for (const auto& m : owed) {
                for (const auto& sst : *all_sstables) {
                    if (sst->generation() == m.generation) {
                        staged.push_back(sst);
                        break;
                    }
                }
            }
            if (!staged.empty()) {
                co_await loader._view_building_worker.local().register_staging_sstable_tasks(
                        std::move(staged), tbl.schema()->id());
            }
        });
        session.pending_register.erase(gid.tablet.value());
    }
    co_return taken;
}

// Same-host path for the sstables which straddle the tablet's boundary.
//
// Only part of each belongs to this tablet, so unlike a fully contained one it cannot just be
// moved. But when this node is the primary there is no reason to put the rows on the wire either:
// read the inputs restricted to the tablet's range and write the result straight into a new
// sstable, on the shard which has the directory open and can read them.
//
// A retry re-combines and re-writes, as a retried stream re-sends: the inputs are not consumed as
// they go, because neighbouring tablets still need them. Harmless for ordinary data and wrong for
// counters - the same caveat the streaming path carries.
future<size_t> sstables_loader::combine_local_upload_sstables(locator::global_tablet_id gid,
        shard_id owning_shard, const dht::token_range& tablet_range,
        std::vector<sstables::shared_sstable>& partially_contained, upload_session& session,
        seastar::abort_source& as) {
    if (partially_contained.empty()) {
        co_return 0;
    }
    as.check();
    auto attach_hold = session.attach_gate.hold();
    auto& tbl = _db.local().find_column_family(gid.table);
    auto s = tbl.schema();

    auto state = co_await upload_destination_state(tbl);

    auto pr = dht::to_partition_range(tablet_range);
    auto token_range = pr.transform(std::mem_fn(&dht::ring_position::token));

    auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s, token_range));
    uint64_t estimated_partitions = 0;
    for (auto& sst : partially_contained) {
        estimated_partitions += co_await sst->estimated_keys_for_range(token_range);
        sst_set->insert(sst);
    }

    // Overlapping the tablet by token bounds does not imply holding a partition inside it, and an
    // sstable with no partitions cannot be produced at all - seal_summary() rejects it and the BTI
    // writer dereferences an unset first key. peek() leaves the fragment for write_components() to
    // consume, so nothing is read twice.
    auto permit = co_await _db.local().obtain_reader_permit(tbl, "sstables_loader::upload_combine", db::no_timeout, {});
    auto reader = tbl.make_streaming_reader(s, std::move(permit), pr, sst_set, gc_clock::now());
    bool empty = false;
    std::exception_ptr ex;
    try {
        empty = co_await reader.peek() == nullptr;
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex || empty) {
        // write_components() takes the reader and closes it; on these two paths it never gets it.
        co_await reader.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        llog.debug("Nothing of tablet {} was found in the {} boundary-straddling sstables on shard {}",
                gid, partially_contained.size(), this_shard_id());
        for (auto& in : partially_contained) {
            session.streamed.insert(in.get());
        }
        auto skipped = partially_contained.size();
        partially_contained.clear();
        co_return skipped;
    }

    // owning_shard was read before this transition started, and a migration or resize since then
    // can have moved the tablet. One map read against the whole cost of the rewrite.
    {
        auto erm_now = tbl.get_effective_replication_map();
        const auto& tablets_now = erm_now->get_token_metadata().tablets();
        auto self = _ss.local().get_token_metadata().get_topology().my_host_id();
        std::optional<shard_id> owner_now;
        if (tablets_now.has_tablet_map(gid.table)) {
            const auto& tmap_now = tablets_now.get_tablet_map(gid.table);
            if (gid.tablet.value() < tmap_now.tablet_count()) {
                for (auto&& r : tmap_now.get_tablet_info(gid.tablet).replicas) {
                    if (r.host == self) {
                        owner_now = r.shard;
                        break;
                    }
                }
            }
        }
        if (owner_now != owning_shard) {
            throw std::runtime_error(fmt::format("Tablet {} is no longer owned by shard {} on this "
                    "node (now {}); the tablet map changed before the combine started. Failing the "
                    "transition so it is retried against the current map.",
                    gid, owning_shard,
                    owner_now ? fmt::to_string(*owner_now) : "not a replica here"));
        }
    }

    auto sst = state == sstables::sstable_state::normal
            ? tbl.make_streaming_sstable_for_write() : tbl.make_streaming_staging_sstable();
    auto cfg = tbl.get_sstables_manager().configure_writer(sstables::repair_origin);
    cfg.leave_unsealed = true;

    try {
        co_await sst->write_components(std::move(reader), estimated_partitions, s, cfg, encoding_stats{});
        co_await sst->open_data();
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        co_await sst->unlink();
        co_await coroutine::return_exception_ptr(std::move(ex));
    }

    // The last point at which the result can still be discarded. An abort promises that what was
    // not consumed stays in the upload directory.
    if (as.abort_requested()) {
        llog.info("Discarding the combined sstable of tablet {} on shard {}: the request was "
                "aborted while it was being written", gid, this_shard_id());
        co_await sst->unlink();
        throw seastar::abort_requested_exception();
    }

    auto taken = partially_contained.size();
    auto generation = sst->generation();
    auto version = sst->get_version();
    auto format = sst->get_format();
    llog.debug("Combined {} boundary-straddling sstables of tablet {} into one on shard {}, attaching on shard {}",
            taken, gid, this_shard_id(), owning_shard);

    std::exception_ptr attach_ex;
    std::exception_ptr add_ex;
    try {
        add_ex = co_await container().invoke_on(owning_shard, [gid, state, generation, version, format]
                (sstables_loader& loader) -> future<std::exception_ptr> {
            auto& tbl = loader._db.local().find_column_family(gid.table);
            auto& sst_manager = tbl.get_sstables_manager();
            auto sst = sst_manager.make_sstable(tbl.schema(), tbl.get_storage_options(), generation,
                    state, version, format);
            sst->set_sstable_level(0);
            auto units = co_await sst_manager.dir_semaphore().get_units(1);
            sstables::sstable_open_config ocfg {
                .unsealed_sstable = true,
            };
            // Held for the duration of the load, not read inline: load() takes the sharder by
            // reference and the sharder belongs to the map, which an upload replaces constantly.
            auto erm = tbl.get_effective_replication_map();
            auto s = tbl.schema();
            co_await sst->load(erm->get_sharder(*s), ocfg);

            // owning_shard and tablet_range were captured before the combine, but the sharder above
            // is read live. If the tablet has since moved, add_new_sstable_and_update_cache() would
            // trip belongs_to_other_shard and take the node down. Refuse; the retry recombines.
            const auto& shards = sst->get_shards_for_this_sstable();
            if (shards.size() != 1 || shards[0] != this_shard_id()) {
                throw std::runtime_error(fmt::format(
                        "Combined sstable of tablet {} belongs to shards {{{}}} under the current "
                        "tablet map, not solely to shard {} it is being attached on; the tablet map "
                        "changed during the upload. Failing the transition so it is retried against "
                        "the current map.", gid, fmt::join(shards, ","), this_shard_id()));
            }

            std::exception_ptr add_ex;
            try {
                auto attached = co_await tbl.add_new_sstable_and_update_cache(sst,
                        [&sst_manager, sst] (sstables::shared_sstable loading_sst) -> future<> {
                    if (loading_sst == sst) {
                        auto writer_cfg = sst_manager.configure_writer(loading_sst->get_origin());
                        co_await loading_sst->seal_sstable(writer_cfg.backup);
                    }
                });
                if (state == sstables::sstable_state::staging) {
                    co_await loader._view_building_worker.local().register_staging_sstable_tasks(attached, tbl.schema()->id());
                }
            } catch (...) {
                add_ex = std::current_exception();
            }
            if (add_ex) {
                // The owning shard owns this file's deletion from here. Hand the error back rather
                // than throwing, so the writer does not unlink the same file a second time.
                llog.warn("Attaching the combined sstable of tablet {} on shard {} failed: {}",
                        gid, this_shard_id(), add_ex);
                sst->mark_for_deletion();
            }
            co_return add_ex;
        });
    } catch (...) {
        attach_ex = std::current_exception();
    }
    if (attach_ex) {
        // Nothing was committed - the owning shard failed before taking ownership of the file.
        co_await sst->unlink();
        co_await coroutine::return_exception_ptr(std::move(attach_ex));
    }
    if (add_ex) {
        // Already marked for deletion under the committed name by the owning shard.
        co_await coroutine::return_exception_ptr(std::move(add_ex));
    }

    // The owning shard sealed and attached a separate object over this file, so it is committed.
    // This object still carries the implicit deletion mark get_writer() set for an unsealed
    // sstable, and its destruction below would unlink the committed file under a live reader.
    sst->mark_committed();

    for (auto& in : partially_contained) {
        session.streamed.insert(in.get());
    }
    partially_contained.clear();
    co_return taken;
}

future<upload_replicate_result> sstables_loader::do_upload_replicate_tablet(locator::global_tablet_id gid, shard_id dst_shard) {
    co_await coroutine::switch_to(_sched_group);

    // Named by the coordinator from this node's replica entry, so it has to exist here.
    if (dst_shard >= this_smp_shard_count()) {
        throw std::runtime_error(fmt::format("Upload replication of tablet {} targets shard {}, "
                "but this node has {} shards", gid, dst_shard, this_smp_shard_count()));
    }
    co_await _ss.local().do_tablet_operation(gid, "UploadReplicate", [this, gid, dst_shard] (locator::tablet_metadata_guard& guard) -> future<service::tablet_operation_result> {
        service::session_id session_id;
        locator::host_id primary;
        dht::token_range tablet_range;
        {
            auto& tmap = guard.get_tablet_map();
            auto* trinfo = tmap.get_tablet_transition_info(gid.tablet);
            if (!trinfo) {
                throw std::runtime_error(fmt::format("No transition info for tablet {}", gid));
            }
            if (trinfo->stage != locator::tablet_transition_stage::upload_replicate) {
                throw std::runtime_error(fmt::format("Tablet {} stage is not at upload_replicate", gid));
            }
            if (!trinfo->session_id) {
                throw std::runtime_error(fmt::format("Upload replication of tablet {} was aborted", gid));
            }
            if (!trinfo->upload_info) {
                throw std::runtime_error(fmt::format("Upload replicate transition of tablet {} has no endpoints", gid));
            }
            session_id = trinfo->session_id;
            primary = trinfo->upload_info->primary_host;
            tablet_range = tmap.get_token_range(gid.tablet);
        }

        utils::get_local_injector().inject("upload_replicate_fail", [] {
            throw std::runtime_error("Injected upload_replicate failure");
        });

        service::session_topology_guard session_guard(session_id);
        auto& tbl = _db.local().find_column_family(gid.table);
        auto stream_guard = tbl.stream_in_progress();
        auto ops_id = streaming::file_stream_id::create_random_id();
        auto self = _ss.local().get_token_metadata().get_topology().my_host_id();

        llog.debug("upload_replicate[{}] streaming tablet {} range {} from {} to {}:{}",
                ops_id, gid, tablet_range, primary, self, dst_shard);
        auto resp = co_await streaming::tablet_stream_files(ops_id, tbl, tablet_range, primary, self,
                dst_shard, _messaging, session_guard.abort_source(),
                service::frozen_topology_guard(session_id.uuid()));
        llog.debug("upload_replicate[{}] tablet {} replicated, {} bytes", ops_id, gid, resp.stream_bytes);
        co_return service::tablet_operation_empty_result{};
    });
    co_return upload_replicate_result{};
}

future<upload_tablet_result> sstables_loader::do_upload_tablet(locator::global_tablet_id gid,
        std::vector<uint32_t> source_shards) {
    co_await coroutine::switch_to(_sched_group);

    // Recorded when PREPARE scanned this node and arriving back over the wire. A node restarted
    // with fewer shards still has rows naming shards it no longer has, and invoke_on() past
    // the shard count indexes out of bounds. Rejected before the barrier, so the coordinator
    // sees a failed transition instead of the node going down.
    for (auto shard : source_shards) {
        if (shard >= this_smp_shard_count()) {
            throw std::runtime_error(fmt::format("Upload of tablet {} names source shard {}, but "
                    "this node has {} shards. The request was prepared against a different shard "
                    "count; abort it and start a new one.", gid, shard, this_smp_shard_count()));
        }
    }
    // do_tablet_operation() takes a group0 read barrier and group0 only exists on shard 0. The
    // streaming runs on the shards which opened the sstables, each taking its own guard there.
    co_await _ss.local().do_tablet_operation(gid, "Upload", [this, gid, source_shards = std::move(source_shards)]
            (locator::tablet_metadata_guard&) -> future<service::tablet_operation_result> {
        // Sequential: the shards write to the same destination replica, and the batch was sized on
        // the total.
        for (auto shard : source_shards) {
            co_await container().invoke_on(shard, [gid] (sstables_loader& loader) {
                return loader.stream_tablet_from_upload_dir(gid);
            });
        }
        co_return service::tablet_operation_empty_result{};
    });
    co_return upload_tablet_result{};
}

future<> sstables_loader::stream_tablet_from_upload_dir(locator::global_tablet_id gid) {
    auto& tbl = _db.local().find_column_family(gid.table);
    // table::stop() waits for this, so a DROP TABLE during the transition cannot pull the table
    // out from under the reads and writes below; the metadata guard alone keeps only the object.
    auto stream_guard = tbl.stream_in_progress();
    locator::tablet_metadata_guard guard(tbl, gid);

    // Copied out before the first deferring point: get_tablet_map() is only valid until then, and
    // the transition info it hands back is a pointer into that map.
    service::session_id session_id;
    locator::tablet_upload_info upload_info;
    dht::token_range tablet_range;
    utils::UUID request_id;
    shard_id primary_shard = 0;
    {
        auto& tmap = guard.get_tablet_map();
        auto* trinfo = tmap.get_tablet_transition_info(gid.tablet);
        if (!trinfo) {
            throw std::runtime_error(fmt::format("No transition info for tablet {}", gid));
        }
        if (trinfo->stage != locator::tablet_transition_stage::upload) {
            throw std::runtime_error(fmt::format("Tablet {} stage is not at upload", gid));
        }
        if (!trinfo->session_id) {
            throw std::runtime_error(fmt::format("Upload of tablet {} was aborted", gid));
        }
        if (!trinfo->upload_info) {
            throw std::runtime_error(fmt::format("Upload transition of tablet {} has no endpoints", gid));
        }
        if (!trinfo->upload_info->primary_host) {
            // Guards against a field being added to the transition without being persisted: the
            // default is a null host, whose address lookup fails a long way from the cause.
            throw std::runtime_error(fmt::format("Upload transition of tablet {} has no target replica", gid));
        }
        session_id = trinfo->session_id;
        upload_info = *trinfo->upload_info;
        request_id = upload_info.request_id;
        tablet_range = tmap.get_token_range(gid.tablet);
        if (!request_id) {
            throw std::runtime_error(fmt::format("Upload transition of tablet {} names no request", gid));
        }
        // From the replica set rather than the transition, so a tablet which migrated since is
        // streamed to where it now lives.
        auto& tinfo = tmap.get_tablet_info(gid.tablet);
        auto primary = std::ranges::find_if(tinfo.replicas,
                [&] (auto& r) { return r.host == upload_info.primary_host; });
        if (primary == tinfo.replicas.end()) {
            throw std::runtime_error(fmt::format("Upload target {} is no longer a replica of tablet {}",
                    upload_info.primary_host, gid));
        }
        primary_shard = primary->shard;
    }

    service::session_topology_guard session_guard(session_id);

    co_await ensure_upload_session(request_id, gid.table);
    auto it = _upload_sessions.find(request_id);
    if (it == _upload_sessions.end()) {
        throw std::runtime_error(fmt::format("No upload session {} on shard {}", request_id, this_shard_id()));
    }
    auto session = it->second;
    // What an earlier transition already took out is no longer a candidate: upload RPCs are
    // retried, and without this the retry re-selects a moved file and the node aborts on the
    // missing TOC.
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(session->sstables.size());
    for (const auto& sst : session->sstables) {
        if (!session->consumed.contains(sst.get()) && !session->unlinked.contains(sst.get())) {
            candidates.push_back(sst);
        }
    }
    auto [fully, partially] = co_await get_sstables_for_tablet(candidates, tablet_range,
            [] (const auto& sst) { return sst->get_first_decorated_key().token(); },
            [] (const auto& sst) { return sst->get_last_decorated_key().token(); });

    // Not the classification alone: a previous attempt may have moved sstables out and failed
    // before attaching them. Returning here would retire their work row and lose them.
    auto pending_it = session->pending_attach.find(gid.tablet.value());
    bool has_pending_attach = pending_it != session->pending_attach.end()
            && !pending_it->second.empty();
    auto register_it = session->pending_register.find(gid.tablet.value());
    bool has_pending_register = register_it != session->pending_register.end()
            && !register_it->second.empty();

    if (fully.empty() && partially.empty() && !has_pending_attach && !has_pending_register) {
        llog.debug("Upload of tablet {} has nothing to stream on shard {}", gid, this_shard_id());
        co_return;
    }
    if (has_pending_attach) {
        llog.info("Upload of tablet {} on shard {} is resuming {} unfinished attach(es)",
                gid, this_shard_id(), pending_it->second.size());
    }

    llog.debug("Uploading tablet {} from shard {}: {} fully contained, {} partially contained sstables",
            gid, this_shard_id(), fully.size(), partially.size());

    // Held open by tests across node joins and coordinator handovers. The timeout is a last
    // resort only: wait_for_message() reports it via on_internal_error, which aborts the node
    // under the test harness, so it must exceed every deadline a test can wait under.
    co_await utils::get_local_injector().inject("upload_tablet_before_transport",
            utils::wait_for_message(std::chrono::minutes(10)));

    // Kept for the removal pass at the end, once the transports have consumed the vectors.
    auto partially_streamed = partially;

    auto self = _ss.local().get_token_metadata().get_topology().my_host_id();
    if (upload_info.primary_host == self) {
        // The wire transports below take the session's abort source; the local attach is a few
        // renames checked here, and combine checks it again around its rewrite.
        session_guard.check();
        // primary_shard was resolved from the replica list above, which cannot change while the
        // tablet's own transition is in flight - this host owns the tablet at that shard.
        // Called even with nothing fully contained: only attach knows about sstables a previous
        // attempt moved but did not attach.
        auto taken = co_await attach_local_upload_sstables(gid, primary_shard, fully, *session);
        if (taken) {
            llog.debug("Attached {} fully contained sstables locally for tablet {}", taken, gid);
        }
        if (!partially.empty()) {
            co_await combine_local_upload_sstables(gid, primary_shard, tablet_range, partially, *session,
                    session_guard.abort_source());
        }
    }

    // Different host: send fully contained sstables as files rather than decoding them into
    // mutations and re-encoding them on the far side.
    if (upload_info.primary_host != self && !fully.empty()) {
        auto target_state = co_await upload_destination_state(tbl);

        auto ops_id = streaming::file_stream_id::create_random_id();
        auto dst = raft::server_id(upload_info.primary_host.uuid());

        co_await ser::sstables_loader_rpc_verbs::send_upload_stream_session(
                &_messaging, upload_info.primary_host, dst, ops_id.uuid(), true);
        auto close_session = seastar::defer([this, host = upload_info.primary_host, dst, ops_id] () noexcept {
            // send_message() resolves the host's address before it returns a future, and throws
            // if the host has since left the address map; in a noexcept body that is a crash.
            try {
                (void)ser::sstables_loader_rpc_verbs::send_upload_stream_session(
                        &_messaging, host, dst, ops_id.uuid(), false)
                    .handle_exception([ops_id] (std::exception_ptr ex) {
                        llog.warn("Failed to close upload stream session {}: {}", ops_id, ex);
                        return upload_stream_session_result{};
                    });
            } catch (...) {
                llog.warn("Failed to close upload stream session {}: {}", ops_id, std::current_exception());
            }
        });

        auto permit = co_await _db.local().obtain_reader_permit(tbl, "sstables_loader::upload_file_stream", db::no_timeout, {});
        auto infos = co_await streaming::make_sstable_stream_infos(tbl, fully, ops_id, std::move(permit), target_state);

        // Where the sstables are attached, so it has to own the tablet. Not where the bytes are
        // written: messaging listens with load_balancing_algorithm::port, so the receiving shard is
        // whichever the STREAM_BLOB connection landed on.
        std::vector<streaming::node_and_shard> targets{
            streaming::node_and_shard{upload_info.primary_host, primary_shard}};
        llog.debug("upload_file_stream[{}] sending {} fully contained sstables of tablet {} to {}, attaching on shard {}",
                ops_id, fully.size(), gid, upload_info.primary_host, primary_shard);
        // No topology guard on the wire: the receiver validates one against its own session table,
        // and the upload request's session is not open there. Abort therefore takes effect on the
        // sending side, via the session's abort source, checked between chunks.
        auto bytes = co_await streaming::tablet_stream_files(_messaging, std::move(infos), std::move(targets),
                gid.table, ops_id, service::null_topology_guard, false, &session_guard.abort_source());
        llog.debug("upload_file_stream[{}] tablet {} sent, {} bytes", ops_id, gid, bytes);

        for (auto& sst : fully) {
            if (!session->unlinked.contains(sst.get())) {
                session->unlinked.insert(sst.get());
                co_await sst->unlink();
            }
        }
        fully.clear();
    }

    // Whatever is left goes over the network - nothing, when this node is the primary. A retried
    // transition re-sends these: they straddle the boundary, so unlike the fully contained ones
    // they are not consumed as they go. Replaying a mutation is harmless for ordinary data but not
    // for counters, and here the coordinator retries automatically.
    if (!fully.empty() || !partially.empty()) {
        auto owned = fully;
        owned.insert(owned.end(), partially.begin(), partially.end());
        auto streamer = tablet_sstable_streamer(_messaging, _db, gid.table, guard.get_erm(), std::move(owned),
                primary_replica_only::no, unlink_sstables::no, stream_scope::all);
        streamer.set_explicit_target(upload_info.primary_host);
        streamer.set_abort_source(session_guard.abort_source());
        co_await streamer.stream_one_tablet(tablet_range, std::move(fully), std::move(partially), {});
    }

    // A straddler can only go once this shard has streamed every tablet it overlaps, which is
    // knowable locally because work items are per (node, shard, tablet). The space comes back only
    // at teardown: the session keeps its own reference, and dropping it here is not possible while
    // the sstable list is immutable.
    session->completed_tablets.insert(gid.tablet.value());
    for (auto& sst : partially_streamed) {
        session->streamed.insert(sst.get());
    }

    // Decide before removing: the tablet map reference is only valid until the next deferring
    // point, and unlinking defers.
    std::vector<sstables::shared_sstable> to_remove;
    {
        auto& tmap_now = guard.get_tablet_map();
        for (auto& sst : partially_streamed) {
            if (session->unlinked.contains(sst.get())) {
                continue;
            }
            auto first_tablet = tmap_now.get_tablet_id(sst->get_first_decorated_key().token());
            auto last_tablet = tmap_now.get_tablet_id(sst->get_last_decorated_key().token());
            bool all_done = true;
            for (auto id = first_tablet; id <= last_tablet; id = locator::tablet_id(id.value() + 1)) {
                if (!session->completed_tablets.contains(id.value())) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) {
                session->unlinked.insert(sst.get());
                to_remove.push_back(sst);
            }
        }
    }
    for (auto& sst : to_remove) {
        llog.debug("Marking upload sstable {} for deletion: all overlapping tablets streamed on shard {}",
                sst->get_filename(), this_shard_id());
        sst->mark_for_deletion();
    }
}

future<sstables_loader::upload_measurement> sstables_loader::measure_upload_slice(::table_id table_id,
        std::vector<sstables::shared_sstable> ssts) {
    upload_measurement out;
    out.sstable_count = ssts.size();

    // Named local: the tablet map below is borrowed from it and must not outlive it.
    auto& tbl = _db.local().find_column_family(table_id);
    auto erm = tbl.get_effective_replication_map();
    out.topology_version = erm->get_token_metadata().get_version();
    const auto& tmap = erm->get_token_metadata().tablets().get_tablet_map(table_id);

    // Walk sstables rather than tablets: pairing every tablet against every sstable would be
    // O(tablets x sstables) with an index probe per pair. Mapping first and last token to tablet
    // ids bounds the work by real overlaps.
    for (auto& sst : ssts) {
        co_await coroutine::maybe_yield();

        // A 'ka' component is named <ks>-<cf>-ka-<numeric generation>-<component>: neither the
        // file-streaming rename nor pick_up_from_upload() can give it the generation a tablet
        // table needs, and its numeric one collides across source nodes. Refused here, before
        // any work exists, so the request fails with nothing consumed.
        if (sst->get_version() == sstables::sstable_version_types::ka) {
            throw std::runtime_error(fmt::format("{} is a legacy 'ka' format sstable, which cluster upload "
                    "cannot rename into a tablet table; load it with nodetool refresh --load-and-stream instead",
                    sst->get_filename()));
        }

        auto first = sst->get_first_decorated_key().token();
        auto last = sst->get_last_decorated_key().token();
        auto first_tablet = tmap.get_tablet_id(first);
        auto last_tablet = tmap.get_tablet_id(last);

        auto touch = [&] (locator::tablet_id id) -> upload_work_item& {
            auto& item = out.items[id.value()];
            item.tablet_id = id.value();
            if (item.shards.empty()) {
                item.shards.push_back(this_shard_id());
                item.shard_bytes.push_back(0);
            }
            return item;
        };

        if (first_tablet == last_tablet) {
            auto& item = touch(first_tablet);
            item.estimated_bytes += sst->data_size();
            item.shard_bytes.back() += sst->data_size();
            continue;
        }

        // Probing the index per tablet costs a lookup - with BTI, I/O - per (sstable, tablet) pair,
        // and sstables from a Cassandra node routinely span the whole ring. Apportion by token
        // width beyond a few tablets; the scheduler treats these as estimates anyway.
        constexpr uint64_t max_index_probes_per_sstable = 8;
        auto tablets_spanned = last_tablet.value() - first_tablet.value() + 1;
        bool probe = tablets_spanned <= max_index_probes_per_sstable;

        auto sst_range = dht::token_range::make(first, last);

        for (auto id = first_tablet; id <= last_tablet; id = locator::tablet_id(id.value() + 1)) {
            co_await coroutine::maybe_yield();
            auto& item = touch(id);
            if (probe) {
                auto bytes = co_await sst->estimated_data_size_for_range(tmap.get_token_range(id));
                item.estimated_bytes += bytes;
                item.shard_bytes.back() += bytes;
                continue;
            }
            auto bytes = uint64_t(sst->data_size() * dht::overlap_ratio(sst_range, tmap.get_token_range(id)));
            if (!bytes) {
                continue;
            }
            item.estimated_bytes += bytes;
            item.shard_bytes.back() += bytes;
        }
    }

    co_return std::move(out);
}

// Waits for this node's own view of the tablet map to catch up - never for the coordinator to act,
// which is why prepare_upload() cannot use await_topology_quiesced_and_get_erm(). That helper
// waits until the coordinator observes an empty balance plan, but prepare_upload is a verb the
// coordinator awaits on its own fiber, so the two deadlock and its main loop parks for good. A
// read barrier plus a version comparison needs nothing from it, and the map cannot move while it
// is parked here.
future<locator::effective_replication_map_ptr> sstables_loader::await_local_tablet_map_caught_up(::table_id table_id) {
    constexpr auto poll_interval = std::chrono::milliseconds(100);
    constexpr auto max_attempts = 300;
    for (int attempt = 0; ; attempt++) {
        auto& t = _db.local().find_column_family(table_id);
        auto erm = t.get_effective_replication_map();
        if (!t.uses_tablets()) {
            co_return erm;
        }
        if (co_await _ss.local().verify_topology_quiesced(erm->get_token_metadata().get_version())) {
            co_return erm;
        }
        if (attempt >= max_attempts) {
            throw std::runtime_error(fmt::format(
                    "Timed out waiting for the tablet map of table {} to settle on this node; "
                    "cluster upload cannot measure work against a moving map", table_id));
        }
        erm = nullptr;
        co_await seastar::sleep(poll_interval);
    }
}

future<prepare_upload_response> sstables_loader::prepare_upload(::table_id table_id) {
    if (this_shard_id() != 0) {
        co_return co_await container().invoke_on(0, [table_id] (sstables_loader& loader) {
            return loader.prepare_upload(table_id);
        });
    }

    auto units = co_await get_units(_prepare_upload_sem, 1);
    co_await coroutine::switch_to(_sched_group);

    // Measuring must observe the tablet boundaries the scheduler will later slice the work by;
    // resize stays blocked for the request, so the map does not move in between. The directory is
    // not frozen, though, and the work list is fixed here: files added after this are picked up
    // only for tablets that happen to be scheduled anyway - a partial, silent inclusion.
    auto erm = co_await await_local_tablet_map_caught_up(table_id);
    auto& tbl = _db.local().find_column_family(table_id);
    auto s = tbl.schema();

    if (!tbl.uses_tablets()) {
        throw std::runtime_error(fmt::format("Table {}.{} does not use tablets, cluster upload is not applicable",
                s->ks_name(), s->cf_name()));
    }

    auto expected_topology_version = erm->get_token_metadata().get_version();

    sstables::sstable_open_config cfg {
        .load_bloom_filter = false,
    };
    // The only window in which a request is queued or preparing and no work rows exist yet.
    // See upload_tablet_before_transport: the timeout aborts the node, so keep it unreachable.
    co_await utils::get_local_injector().inject("upload_prepare_before_scan",
            utils::wait_for_message(std::chrono::minutes(10)));

    auto [tid, sstables_on_shards] = co_await replica::distributed_loader::get_sstables_from_upload_dir(
            _db, s->ks_name(), s->cf_name(), cfg, /* need_mutate_level = */ false);

    // Each slice was opened by that shard's sstables_manager and only that shard may touch it, so
    // measure every slice on its owner and merge the tallies here.
    std::vector<upload_measurement> per_shard(this_smp_shard_count());
    co_await container().invoke_on_all([&per_shard, &sstables_on_shards, table_id] (sstables_loader& loader) -> future<> {
        per_shard[this_shard_id()] = co_await loader.measure_upload_slice(table_id, std::move(sstables_on_shards[this_shard_id()]));
    });

    prepare_upload_response resp;

    // One unit of work per (node, tablet), with the shards holding it collected into the item.
    // Keying per shard too would multiply the group0 work list by the shard count.
    std::map<uint64_t, upload_work_item> items;

    for (shard_id shard = 0; shard < per_shard.size(); shard++) {
        auto& m = per_shard[shard];
        if (m.topology_version != expected_topology_version) {
            throw std::runtime_error(fmt::format("Tablet map moved during the upload scan of {}.{}: "
                    "shard {} measured against topology version {}, shard 0 against {}",
                    s->ks_name(), s->cf_name(), shard, m.topology_version, expected_topology_version));
        }
        resp.sstable_count += m.sstable_count;
        for (auto& [id, measured] : m.items) {
            auto& item = items[id];
            item.tablet_id = measured.tablet_id;
            item.estimated_bytes += measured.estimated_bytes;
            item.shards.insert(item.shards.end(), measured.shards.begin(), measured.shards.end());
            item.shard_bytes.insert(item.shard_bytes.end(), measured.shard_bytes.begin(), measured.shard_bytes.end());
        }
    }

    resp.items.reserve(items.size());
    for (auto& [key, item] : items) {
        resp.total_estimated_bytes += item.estimated_bytes;
        resp.items.push_back(std::move(item));
    }

    llog.info("prepare_upload: table={}.{} sstables={} tablets_with_work={} estimated_bytes={}",
            s->ks_name(), s->cf_name(), resp.sstable_count, resp.items.size(), resp.total_estimated_bytes);
    co_return resp;
}

future<tasks::task_id> sstables_loader::download_new_sstables(sstring ks_name, sstring cf_name,
            sstring prefix, std::vector<sstring> sstables,
            sstring endpoint, sstring bucket, stream_scope scope, bool primary_replica) {
    if (!_storage_manager.is_known_endpoint(endpoint)) {
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    llog.info("Restore sstables from {}({}) to {}.{} using scope={}, primary_replica={}", endpoint, prefix, ks_name, cf_name, scope, primary_replica);

    auto task = co_await _task_manager_module->make_and_start_task<download_task_impl>({}, container(), std::move(endpoint), std::move(bucket), std::move(ks_name), std::move(cf_name),
                                                                                       std::move(prefix), std::move(sstables), scope, primary_replica_only(primary_replica));
    co_return task->id();
}

future<sstables::shared_sstable> sstables_loader::attach_sstable(table_id tid, const minimal_sst_info& min_info) const {
    auto& db = _db.local();
    auto& table = db.find_column_family(tid);
    llog.debug("Adding downloaded SSTable gen={} to the table {}.{} on shard {}", min_info.generation, table.schema()->ks_name(), table.schema()->cf_name(), this_shard_id());
    auto& sst_manager = table.get_sstables_manager();
    auto sst = sst_manager.make_sstable(
        table.schema(), table.get_storage_options(), min_info.generation, sstables::sstable_state::normal, min_info.version, min_info.format);
    sst->set_sstable_level(0);
    auto erm = table.get_effective_replication_map();
    sstables::sstable_open_config cfg {
        .unsealed_sstable = true,
    };
    co_await sst->load(erm->get_sharder(*table.schema()), cfg);
    if (!sst->sstable_identifier()) {
        on_internal_error(llog, "sstable identifier is required for tablet restore");
    }
    co_await table.add_new_sstable_and_update_cache(sst, [&sst_manager, sst] (sstables::shared_sstable loading_sst) -> future<> {
        if (loading_sst == sst) {
            auto writer_cfg = sst_manager.configure_writer(loading_sst->get_origin());
            co_await loading_sst->seal_sstable(writer_cfg.backup);
        }
    });
    co_return sst;
}

// Joins a backup location's prefix with a path that is relative to it, the same
// way cluster-wide backup composes its object keys (db/snapshot/cluster_backup.cc).
static sstring join_path(std::string_view prefix, std::string_view relative_path) {
    if (prefix.empty()) {
        return sstring(relative_path);
    }
    return fmt::format("{}/{}", prefix, relative_path);
}

future<> sstables_loader::download_tablet_sstables(locator::global_tablet_id tid, locator::tablet_metadata_guard& guard) {
    auto& tmap = guard.get_tablet_map();

    auto* trinfo = tmap.get_tablet_transition_info(tid.tablet);
    if (!trinfo) {
        throw std::runtime_error(fmt::format("No transition info for tablet {}", tid));
    }
    if (!trinfo->session_id) {
        throw std::runtime_error(fmt::format("Restore of tablet {} was aborted", tid));
    }
    if (trinfo->snapshot_name.empty()) {
        throw std::runtime_error(format("No snapshot name for tablet {} restore transition", tid));
    }

    // Aborting the restore closes the session, which fires this guard's abort source and
    // interrupts the downloads below.
    service::session_topology_guard session_guard(trinfo->session_id);

    sstring snapshot_name = trinfo->snapshot_name;

    auto s = _db.local().find_schema(tid.table);
    auto tablet_range = tmap.get_token_range(tid.tablet);
    const auto& topo = guard.get_token_metadata()->get_topology();
    auto keyspace_name = s->ks_name();
    auto table_name = s->cf_name();
    auto datacenter = topo.get_datacenter();
    auto rack = topo.get_rack();

    db::snapshot_table_helper sth(_sys_dist_ks.qp());

    auto snapshot_info = co_await sth.get_snapshot_remote_location(snapshot_name, datacenter);
    llog.info("Downloading sstables for tablet {} from {}@{}/{}", tid, snapshot_name, snapshot_info.endpoint, snapshot_info.bucket);
    auto sst_infos = co_await sth.get_snapshot_sstables(snapshot_name, keyspace_name, table_name, datacenter, rack,
            db::consistency_level::LOCAL_QUORUM, tablet_range.start().transform([] (auto& v) { return v.value(); }), tablet_range.end().transform([] (auto& v) { return v.value(); }));
    llog.debug("{} SSTables found for tablet {}", sst_infos.size(), tid);
    if (sst_infos.empty()) {
        // It can happen when the restored table has more tablets than the original.
        // Some tablets simply have no data in their token range.
        llog.info("No SSTables found for tablet {}, skipping", tid);
        co_return;
    }

    // Skip sstables a previous restore attempt already fetched so a re-entered
    // or retried restore doesn't re-download what's already in place.
    auto pending = sst_infos | std::views::filter([] (const auto& si) { return si.downloaded == db::is_downloaded::no; });
    auto [ fully, partially ] = co_await get_sstables_for_tablet(pending, tablet_range, [] (const auto& si) { return si.first_token; }, [] (const auto& si) { return si.last_token; });
    if (!partially.empty()) {
        llog.debug("Sstable {} is partially contained", partially.front().sstable_id);
        throw std::logic_error("sstables_partially_contained");
    }
    llog.debug("{} SSTables filtered by range {} for tablet {}", fully.size(), tablet_range, tid);
    co_await utils::get_local_injector().inject("pause_tablet_restore", utils::wait_for_message(60s));

    if (fully.empty()) {
        // It can happen that a tablet exists and contains no data. Just skip it
        co_return;
    }

    std::unordered_map<sstring, std::vector<sstring>> toc_names_by_prefix;
    for (const auto& e : fully) {
        // e.prefix is relative to the backup location's own prefix (snapshot_remote_locations.prefix).
        toc_names_by_prefix[join_path(snapshot_info.prefix, e.prefix)].emplace_back(e.toc_name);
    }

    auto ep_type = _storage_manager.get_endpoint_type(snapshot_info.endpoint);
    sstables::sstable_open_config cfg {
        .load_bloom_filter = false,
    };

    using sstables_col = std::vector<sstables::shared_sstable>;
    using prefix_sstables = std::vector<sstables_col>;

    // Per-shard abort sources for the object-store download pipeline (cf. download_task_impl::run).
    // They are wired to the session abort only after the sstables are opened (see below).
    std::vector<seastar::abort_source> shard_aborts(this_smp_shard_count());

    auto sstables_on_shards = co_await map_reduce(toc_names_by_prefix, [&] (auto ent) {
        return replica::distributed_loader::get_sstables_from_object_store(_db, s->ks_name(), s->cf_name(),
                std::move(ent.second), snapshot_info.endpoint, ep_type, snapshot_info.bucket, std::move(ent.first), cfg, [&] {
                    return &shard_aborts[this_shard_id()];
                }).then_unpack([] (table_id, auto sstables) {
                    return make_ready_future<std::vector<sstables_col>>(std::move(sstables));
                });
    }, std::vector<prefix_sstables>(this_smp_shard_count()), [&] (std::vector<prefix_sstables> a, std::vector<sstables_col> b) {
        // We can't move individual elements of b[i], because these
        // are lw_shared_ptr-s collected on another shard. So we move
        // the whole sstables_col here so that subsequent code will
        // walk over it and move the pointers where it wants on proper
        // shard.
        for (unsigned i = 0; i < this_smp_shard_count(); i++) {
            a[i].push_back(std::move(b[i]));
        }
        return a;
    });

    // Wire the session abort into the download sources only now, after opening: an aborted
    // metadata read would be misreported as sstable corruption, so opening above must stay
    // non-abortable.
    named_gate g("sstables_loader::download_tablet_sstables");
    std::exception_ptr ex;
    try {
        auto& session_as = session_guard.abort_source();
        session_as.check();
        auto sub = session_as.subscribe([&shard_aborts, &g, &session_as] () noexcept {
            try {
                auto h = g.hold();
                (void)smp::invoke_on_all([&shard_aborts, ex = session_as.abort_requested_exception_ptr()] {
                    shard_aborts[this_shard_id()].request_abort_ex(ex);
                }).finally([h = std::move(h)] {});
            } catch (...) {
            }
        });

        auto downloaded_ssts = co_await container().map_reduce0(
            [tid, &sstables_on_shards](auto& loader) -> future<std::vector<std::vector<minimal_sst_info>>> {
                sstables_col sst_chunk;
                for (auto& psst : sstables_on_shards[this_shard_id()]) {
                    for (auto&& sst : psst) {
                        sst_chunk.push_back(std::move(sst));
                    }
                }
                std::vector<std::vector<minimal_sst_info>> local_min_infos(this_smp_shard_count());
                co_await max_concurrent_for_each(sst_chunk, 16, [&loader, tid, &local_min_infos](const auto& sst) -> future<> {
                    auto& table = loader._db.local().find_column_family(tid.table);
                    auto stream_guard = table.stream_in_progress();
                    auto min_info = co_await download_sstable(loader._db.local(), table, sst, llog);
                    local_min_infos[min_info.shard].emplace_back(std::move(min_info));
                });
                co_return local_min_infos;
            },
            std::vector<std::vector<minimal_sst_info>>(this_smp_shard_count()),
            [](auto init, auto&& item) -> std::vector<std::vector<minimal_sst_info>> {
                for (std::size_t i = 0; i < item.size(); ++i) {
                    init[i].append_range(std::move(item[i]));
                }
                return init;
            });

        co_await container().invoke_on_all([tid, &downloaded_ssts, snap_name = snapshot_name, keyspace_name, table_name, datacenter, rack] (auto& loader) -> future<> {
            auto shard_ssts = std::move(downloaded_ssts[this_shard_id()]);
            db::snapshot_table_helper sth(loader._sys_dist_ks.qp());
            co_await max_concurrent_for_each(shard_ssts, 16, [&sth, &loader, tid, snap_name, keyspace_name, table_name, datacenter, rack](const auto& min_info) -> future<> {
                sstables::shared_sstable attached_sst = co_await loader.attach_sstable(tid.table, min_info);
                co_await sth.update_sstable_download_status(snap_name,
                                                            keyspace_name,
                                                            table_name,
                                                            datacenter,
                                                            rack,
                                                            *attached_sst->sstable_identifier(),
                                                            attached_sst->get_first_decorated_key().token(),
                                                            db::is_downloaded::yes);
            });
        });
    } catch (...) {
        ex = std::current_exception();
    }

    // Drain any in-flight abort dispatch before shard_aborts/session_as go out of scope.
    co_await g.close();

    if (ex) {
        // An aborted download leaves unconsumed sstables in sstables_on_shards. These are
        // lw_shared_ptr-s owned by their respective shards, so they must be released there
        // rather than while this coroutine frame unwinds on the home shard.
        co_await container().invoke_on_all([&sstables_on_shards] (sstables_loader&) {
            sstables_on_shards[this_shard_id()] = {}; // clear on correct shard
        });
        std::rethrow_exception(ex);
    }
}

future<std::vector<tablet_sstable_collection>> get_sstables_for_tablets_for_tests(const std::vector<sstables::shared_sstable>& sstables,
                                                                                  std::vector<dht::token_range>&& tablets_ranges) {
    return tablet_sstable_streamer::get_sstables_for_tablets(sstables, std::move(tablets_ranges));
}

static future<manifest_summary> process_manifest(input_stream<char>& is, sstring keyspace, sstring table,
                                 const sstring& expected_snapshot_name,
                                 const sstring& manifest_prefix, db::system_distributed_keyspace& sys_dist_ks,
                                 db::consistency_level cl) {
    // Read the entire JSON content
    rjson::chunked_content content = co_await util::read_entire_stream(is);

    rjson::value parsed = rjson::parse(std::move(content));

    // Basic validation that tablet_count is a power of 2, as expected by our restore process
    auto& table_obj = rjson::get(parsed, "table");
    size_t tablet_count = rjson::get<uint64_t>(table_obj, "tablet_count");

    if (!std::has_single_bit(tablet_count)) {
        on_internal_error(llog, fmt::format("Invalid tablet_count {} in manifest {}, expected a power of 2", tablet_count, manifest_prefix));
    }

    // Extract the necessary fields from the manifest
    // Expected JSON structure documented in docs/dev/object_storage.md
    auto& snapshot_obj = rjson::get(parsed, "snapshot");
    auto snapshot_name = rjson::get<std::string>(snapshot_obj, "name");
    if (snapshot_name != expected_snapshot_name) {
        throw std::runtime_error(fmt::format("Manifest {} belongs to snapshot '{}', expected '{}'",
            manifest_prefix, snapshot_name, expected_snapshot_name));
    }
    if (keyspace.empty()) {
        keyspace = rjson::get<std::string>(table_obj, "keyspace_name");
    }
    if (table.empty()) {
        table = rjson::get<std::string>(table_obj, "table_name");
    }
    auto& node_obj = rjson::get(parsed, "node");
    auto datacenter = rjson::get<std::string>(node_obj, "datacenter");
    auto rack = rjson::get<std::string>(node_obj, "rack");

    // Process each sstable entry in the manifest
    // FIXME: cleanup of the snapshot-related rows is needed in case anything throws in here.
    auto sstables = rjson::find(parsed, "sstables");
    if (!sstables) {
        co_return manifest_summary{tablet_count, 0};
    }
    if (!sstables->IsArray()) {
        throw std::runtime_error("Malformed manifest, 'sstables' is not array");
    }

    db::snapshot_table_helper sth(sys_dist_ks.qp());

    for (auto& sstable_entry : sstables->GetArray()) {
        // rjson::get functions assert if the passed value is not an object
        if (!sstable_entry.IsObject()) {
            throw std::runtime_error("Malformed manifest, the entry in the 'sstables' array is not an object");
        }
        auto id = rjson::to_sstable_id(rjson::get(sstable_entry, "id"));
        auto first_token = rjson::to_token(rjson::get(sstable_entry, "first_token"));
        auto last_token = rjson::to_token(rjson::get(sstable_entry, "last_token"));
        auto toc_name = rjson::to_sstring(rjson::get(sstable_entry, "toc_name"));
        auto tablet_id = rjson::get<size_t>(sstable_entry, "tablet_id");
        auto repaired_at = rjson::get<int64_t>(sstable_entry, "repaired_at");
        auto data_size = rjson::get<int64_t>(sstable_entry, "data_size");
        auto index_size = rjson::get<int64_t>(sstable_entry, "index_size");
        auto prefix = sstring(std::filesystem::path(manifest_prefix).parent_path().string());
        // Insert the snapshot sstable metadata into system_distributed.snapshot_sstables with a TTL of 3 days, that should be enough
        // for any snapshot restore operation to complete, and after that the metadata will be automatically cleaned up from the table
        co_await sth.insert_snapshot_sstable(snapshot_name, keyspace, table, datacenter, rack, id, first_token, last_token,
                                             toc_name, prefix, locator::host_id::create_null_id(), tablet_id, db::snapshot_state::remote,
                                             repaired_at, data_size, index_size, cl);
    }

    co_return manifest_summary{tablet_count, sstables->Size()};
}

future<manifest_summary> populate_snapshot_sstables_from_manifests(sstables::storage_manager& sm, db::system_distributed_keyspace& sys_dist_ks, sstring keyspace, sstring table, sstring endpoint, sstring bucket, sstring prefix, sstring expected_snapshot_name, utils::chunked_vector<sstring> manifest_prefixes, db::consistency_level cl) {
    if (manifest_prefixes.empty()) {
        throw std::invalid_argument("manifest prefixes list must not be empty");
    }

    // Download manifests in parallel and populate system_distributed.snapshot_sstables
    // with the content extracted from each manifest
    auto client = sm.get_endpoint_client(endpoint);

    // tablet_count to be returned by this function, we also validate that all manifests passed contain the same tablet count
    std::optional<size_t> tablet_count;
    size_t nr_sstables = 0;

    co_await seastar::max_concurrent_for_each(manifest_prefixes, 16, [&] (const sstring& manifest_prefix) {
        // Manifest entries are relative to the backup location's prefix, same as start_restore.
        sstables::object_name name(bucket, join_path(prefix, manifest_prefix));
        auto source = client->make_download_source(name);
        return seastar::with_closeable(input_stream<char>(std::move(source)), [&] (input_stream<char>& is) {
            return process_manifest(is, keyspace, table, expected_snapshot_name, manifest_prefix, sys_dist_ks, cl).then([&](manifest_summary ms) {
                size_t count = ms.tablet_count;
                if (!tablet_count) {
                    tablet_count = count;
                } else if (*tablet_count != count) {
                    throw std::runtime_error(fmt::format("Inconsistent tablet_count values in manifest {}: expected {}, got {}", manifest_prefix, *tablet_count, count));
                }
                nr_sstables += ms.nr_sstables;
            });
        });
    });

    co_return manifest_summary{
        .tablet_count = *tablet_count,
        .nr_sstables = nr_sstables,
    };
}

class sstables_loader::progress_reporting_task_impl : public tasks::task_manager::task::impl {
protected:
    tasks::task_manager::task::progress _progress;
    // Set by run() with its final figures: an update already in flight when the timer was
    // cancelled must not overwrite them, or a finished task reports completed < total.
    bool _progress_frozen = false;
    seastar::named_gate _gate{"progress_updater"};
    timer<seastar::lowres_clock> _progress_update_timer;

    using tasks::task_manager::task::impl::impl;

    virtual future<> update_progress() = 0;

    void init_progress_timer() {
        _progress_update_timer.set_callback([this] {
            if (auto gh = _gate.try_hold()) {
                std::ignore = update_progress().handle_exception([] (std::exception_ptr ex) {
                    llog.warn("Failed to update task progress: {}", ex);
                }).finally([this, gh = std::move(*gh)] {
                    if (!_gate.is_closed()) {
                        _progress_update_timer.rearm(lowres_clock::now() + 5s);
                    }
                });
            }
        });
    }

    virtual tasks::is_internal is_internal() const noexcept override {
        return tasks::is_internal::no;
    }

    virtual tasks::is_user_task is_user_task() const noexcept override {
        return tasks::is_user_task::yes;
    }

    tasks::is_abortable is_abortable() const noexcept override {
        return tasks::is_abortable::yes;
    }

public:
    future<tasks::task_manager::task::progress> get_progress() const override {
        co_return _progress;
    }

    future<> release_resources() noexcept override {
        _progress_update_timer.cancel();
        co_await _gate.close();
    }
};

class sstables_loader::tablet_restore_task_impl : public sstables_loader::progress_reporting_task_impl {
    sharded<sstables_loader>& _loader;
    table_id _tid;
    sstring _snap_name;
    size_t _tablet_count;

    future<> update_progress() override {
        auto& loader = _loader.local();
        auto& db = loader._db.local();
        auto s = db.find_schema(_tid);
        auto md = db.get_token_metadata_ptr();
        const auto& topo = md->get_topology();
        auto dc = topo.get_datacenter();
        auto dc_racks_it = topo.get_datacenter_racks().find(dc);
        if (dc_racks_it == topo.get_datacenter_racks().end()) {
            co_return;
        }

        db::snapshot_table_helper sth(loader._sys_dist_ks.qp());
        tasks::task_manager::task::progress progress = {};
        co_await max_concurrent_for_each(dc_racks_it->second, 16, [&](const auto& rack_entry) -> future<> {
            auto p = co_await sth.get_snapshot_sstables_progress(_snap_name, s->ks_name(), s->cf_name(), dc, rack_entry.first);
            progress.total += p.nr_sstables;
            progress.completed += p.nr_downloaded_sstables;
        });
        if (_progress_frozen) {
            co_return;
        }
        _progress = progress;
    }

public:
    tablet_restore_task_impl(tasks::task_manager::module_ptr module, sharded<sstables_loader>& loader, sstring ks,
            table_id tid, sstring snap_name, manifest_summary ms) noexcept
        : progress_reporting_task_impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
        , _loader(loader)
        , _tid(std::move(tid))
        , _snap_name(std::move(snap_name))
        , _tablet_count(ms.tablet_count)
    {
        _status.progress_units = "sstables";
        _progress.total = ms.nr_sstables;
        init_progress_timer();
        _progress_update_timer.arm(lowres_clock::now());
    }

    virtual std::string type() const override {
        return "restore_tablets";
    }

    void abort() noexcept override {
        tasks::task_manager::task::impl::abort();
        // Closing the restore sessions makes the in-flight download RPCs fail and the
        // topology coordinator clear the restore transitions, which lets run() (waiting
        // on the topology request) return. Fire-and-forget: the task's own abort source,
        // already triggered above, surfaces the abort_requested error to the caller.
        (void)_loader.local()._ss.local().abort_restore_tablets(_tid).handle_exception([tid = _tid] (std::exception_ptr ex) {
            llog.warn("Failed to abort restore for table {}: {}", tid, ex);
        });
    }

protected:
    virtual future<> run() override {
        auto& loader = _loader.local();

        auto current_schema = loader.local_db().find_schema(_tid);
        auto min_tablet_count = current_schema->tablet_options().min_tablet_count;
        auto max_tablet_count = current_schema->tablet_options().max_tablet_count;
        co_await loader._ss.local().alter_table_with_tablet_hints(_tid, _tablet_count, _tablet_count, true, false, &_as);

        std::exception_ptr eptr;
        try {
            co_await loader._ss.local().restore_tablets(_tid, _snap_name);
        } catch (...) {
            llog.error("Failed to restore tablets for table_id {}. Error: {}", _tid, std::current_exception());
            eptr = std::current_exception();
        }

        try {
            llog.info("Restoring table with tid {} to the original schema", _tid);
            // remove_unset: the table saved nullopt because it had no hint of its own, and passing
            // nullopt would leave it pinned at min == max.
            co_await loader._ss.local().alter_table_with_tablet_hints(_tid, min_tablet_count, max_tablet_count, false, true);
        } catch (...) {
            llog.error("Failed to restore original schema for table_id {}. Error: {}", _tid, std::current_exception());
        }

        if (eptr) {
            std::rethrow_exception(eptr);
        }

        // Restore complete. The total was known upfront from manifest parsing.
        // Mark all progress as complete and stop the background update timer.
        _progress_update_timer.cancel();
        _progress_frozen = true;
        _progress.completed = _progress.total;
    }
};

// Progress counts the rows of system.upload_work still to consume; nothing is added to a request
// once live, so the count only falls.
class sstables_loader::cluster_upload_task_impl : public sstables_loader::progress_reporting_task_impl {
    sharded<sstables_loader>& _loader;
    table_id _tid;
    std::optional<size_t> _target_tablet_count;
    bool _primary_replica_only;
    std::optional<utils::UUID> _request_id;

    future<> update_progress() override {
        if (!_request_id) {
            co_return;
        }
        auto& loader = _loader.local();
        // Counted server-side and restricted to this request's partition: materializing the rows
        // costs nodes x tablets decoded rows every tick, just to take their count.
        uint64_t outstanding = 0;
        co_await loader._sys_dist_ks.qp().query_internal(
                format("SELECT COUNT(tablet_id) AS c FROM system.{} WHERE request_id = {}",
                        db::system_keyspace::UPLOAD_WORK, *_request_id),
                [&outstanding] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
                    outstanding = uint64_t(row.get_as<int64_t>("c"));
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
        if (_progress_frozen) {
            co_return;
        }
        // High-water mark, not first observation: the work list is written across many group0
        // commands, so an early sample would peg progress at zero until the count fell below it.
        _progress.total = std::max(_progress.total, double(outstanding));
        _progress.completed = _progress.total - outstanding;
    }

public:
    cluster_upload_task_impl(tasks::task_manager::module_ptr module, sharded<sstables_loader>& loader,
            sstring ks, sstring table, table_id tid, std::optional<size_t> target_tablet_count,
            bool primary_replica_only) noexcept
        : progress_reporting_task_impl(module, tasks::task_id::create_random_id(), 0, "node", ks, table, "", tasks::task_id::create_null_id())
        , _loader(loader)
        , _tid(std::move(tid))
        , _target_tablet_count(target_tablet_count)
        , _primary_replica_only(primary_replica_only)
    {
        _status.progress_units = "work items";
        init_progress_timer();
    }

    virtual std::string type() const override {
        return "cluster_upload";
    }

    void abort() noexcept override {
        tasks::task_manager::task::impl::abort();
        (void)_loader.local()._ss.local().abort_upload_tablets(_tid).handle_exception([tid = _tid] (std::exception_ptr ex) {
            llog.warn("Failed to abort cluster upload for table {}: {}", tid, ex);
        });
    }

protected:
    virtual future<> run() override {
        auto& loader = _loader.local();
        _progress_update_timer.arm(lowres_clock::now() + 1s);

        // Pre-size before the request exists, so the coordinator's scan measures work against the
        // boundaries the data will land in: uploading into an under-provisioned table produces
        // exactly the oversized tablets this mechanism exists to avoid. min == max == target is the
        // only form alter_table_with_tablet_hints() will wait on, and it has to be put back or the
        // table can never split or merge again.
        std::optional<ssize_t> saved_min_tablet_count;
        std::optional<ssize_t> saved_max_tablet_count;
        bool hints_pinned = false;
        if (_target_tablet_count) {
            // Only the invocation that starts the upload may pre-size. A second --tablets caller
            // would pin the table under the first's load, and since make_resize_plan() skips a
            // table with an ongoing upload its target could never be reached, so it would hang.
            if (co_await loader._ss.local().has_upload_request_for(_tid)) {
                throw std::invalid_argument(fmt::format("Table {} already has an upload in progress; "
                        "--tablets can only be used by the invocation that starts it", _tid));
            }
            auto opts = loader.local_db().find_schema(_tid)->tablet_options();
            saved_min_tablet_count = opts.min_tablet_count;
            saved_max_tablet_count = opts.max_tablet_count;
            llog.info("Pre-sizing table {} to {} tablets before upload", _tid, *_target_tablet_count);
        }

        std::exception_ptr eptr;
        try {
            if (_target_tablet_count) {
                // Pinned before the wait, not after: the schema change lands first, and a wait
                // that then fails or is aborted must still put the hints back below.
                hints_pinned = true;
                co_await loader._ss.local().alter_table_with_tablet_hints(_tid, *_target_tablet_count,
                        *_target_tablet_count, true, false, &_as);
            }
            // abort() cancels the request by table, so an abort that lands before the request
            // exists - during the pre-size above, or before upload_tablets() commits it - finds
            // nothing to cancel. Checked here, and again once the request id is known, so that
            // window ends in a failed task instead of a load that runs to completion.
            co_await utils::get_local_injector().inject("cluster_upload_before_request",
                    [this] (auto& handler) -> future<> {
                        llog.info("cluster_upload_before_request: paused before registering the upload of {}", _tid);
                        co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{10});
                    });
            _as.check();
            co_await loader._ss.local().upload_tablets(_tid, _primary_replica_only, _target_tablet_count,
                    [this] (utils::UUID request_id) -> future<> {
                        _request_id = request_id;
                        // abort() raises _as before it scans for the request, so finding it set
                        // here means the scan may have run before the request was committed.
                        // Repeated now that there is a request to record the abort on.
                        if (_as.abort_requested()) {
                            co_await _loader.local()._ss.local().abort_upload_tablets(_tid);
                        }
                    });
        } catch (...) {
            eptr = std::current_exception();
            llog.error("Cluster upload failed for table_id {}: {}", _tid, eptr);
        }

        if (hints_pinned) {
            try {
                llog.info("Restoring tablet count hints of table {} after upload", _tid);
                // remove_unset: otherwise a table that had no hint keeps this task's pin forever.
                co_await loader._ss.local().alter_table_with_tablet_hints(_tid,
                        saved_min_tablet_count, saved_max_tablet_count, false, true);
            } catch (...) {
                llog.error("Failed to restore tablet count hints of table {}: {}",
                        _tid, std::current_exception());
            }
        }

        _progress_update_timer.cancel();
        if (eptr) {
            std::rethrow_exception(eptr);
        }
        _progress_frozen = true;
        _progress.completed = _progress.total;
    }
};

future<tasks::task_id> sstables_loader::upload_tablets_task(table_id tid, sstring keyspace, sstring table,
        std::optional<size_t> target_tablet_count, bool primary_replica_only) {
    // Rejected before a task exists: pre-sizing waits for the balancer with a bare equality test,
    // no timeout and no abort source (alter_table_with_tablet_hints()' own FIXME, SCYLLADB-1076),
    // so a count the balancer will never produce hangs before the task's abort has anything to
    // cancel.
    if (target_tablet_count) {
        auto opts = _db.local().find_schema(tid)->tablet_options();
        if (opts.pow2_count.value_or(db::tablet_options::default_pow2_count)
                && !std::has_single_bit(*target_tablet_count)) {
            throw std::invalid_argument(fmt::format(
                    "Cannot pre-size {}.{} to {} tablets: the table aligns tablet counts to powers "
                    "of two, so the balancer would never reach that count. Pass a power of two "
                    "({} or {}), or set pow2_count = false on the table.",
                    keyspace, table, *target_tablet_count,
                    std::bit_floor(*target_tablet_count), std::bit_ceil(*target_tablet_count)));
        }
    }
    // Checked at the entry point too, but that is only reached after the pre-size; a
    // mixed-version cluster is turned away before any schema change.
    if (!_db.local().features().tablet_upload) {
        throw std::invalid_argument("Cannot upload cluster-wide: every node must support TABLET_UPLOAD first");
    }
    // A co-located table has no system.tablets partition of its own to hold the upload
    // transitions the coordinator writes, so a request for one can never run: the coordinator
    // would fail the same plan every pass until the request is aborted. Rejected up front,
    // synchronously, like the vnode case.
    {
        const auto& tablets = _ss.local().get_token_metadata().tablets();
        if (tablets.has_tablet_map(tid) && !tablets.is_base_table(tid)) {
            auto base = _db.local().find_schema(tablets.get_base_table(tid));
            throw std::invalid_argument(fmt::format(
                    "Cannot upload into {}.{}: it is co-located with {}.{}, and cluster upload supports "
                    "base tables only; load it with nodetool refresh on each node instead",
                    keyspace, table, base->ks_name(), base->cf_name()));
        }
    }
    llog.info("Starting cluster upload for {}.{}, target_tablet_count={} primary_replica_only={}",
            keyspace, table, target_tablet_count, primary_replica_only);
    auto task = co_await _task_manager_module->make_and_start_task<cluster_upload_task_impl>({}, container(),
            std::move(keyspace), std::move(table), tid, target_tablet_count, primary_replica_only);
    co_return task->id();
}

future<> sstables_loader::abort_upload(table_id tid) {
    // The tasks first: a task that has not registered its request yet (it is pre-sizing the
    // table, or about to commit) is reachable only through its abort source, and its abort()
    // repeats the request-level abort once the request exists. Then the request itself, for
    // callers whose task lives on another node or has no task at all.
    auto s = _db.local().find_schema(tid);
    co_await container().invoke_on_all([ks = s->ks_name(), cf = s->cf_name()] (sstables_loader& loader) {
        for (auto& [id, task] : loader._task_manager_module->get_local_tasks()) {
            if (task->type() != "cluster_upload" || task->is_complete()) {
                continue;
            }
            const auto& st = task->get_status();
            if (st.keyspace == ks && st.table == cf) {
                llog.info("Aborting cluster upload task {} of {}.{}", id, ks, cf);
                task->abort();
            }
        }
    });
    co_await _ss.local().abort_upload_tablets(tid);
}

future<tasks::task_id> sstables_loader::restore_tablets(table_id tid, sstring keyspace, sstring table, sstring snap_name, sstring endpoint, sstring bucket, sstring prefix, utils::chunked_vector<sstring> manifests) {
    auto summary = co_await populate_snapshot_sstables_from_manifests(_storage_manager, _sys_dist_ks, keyspace, table, endpoint, bucket, prefix, snap_name, std::move(manifests));

    auto datacenter = _db.local().get_token_metadata().get_topology().get_datacenter();

    db::snapshot_table_helper sth(_sys_dist_ks.qp());
    // TODO: update state when all restored...
    co_await sth.insert_snapshot_remote_location(snap_name, datacenter, endpoint, bucket, prefix, db::snapshot_state::remote);

    auto task = co_await _task_manager_module->make_and_start_task<tablet_restore_task_impl>({}, container(), keyspace, tid, std::move(snap_name), summary);
    co_return task->id();
}
