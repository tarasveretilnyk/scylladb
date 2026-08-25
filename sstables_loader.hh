/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <map>
#include <vector>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include "dht/i_partitioner_fwd.hh"
#include "dht/token.hh"
#include "schema/schema_fwd.hh"
#include "sstables/generation_type.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/version.hh"
#include "tasks/task_manager.hh"
#include "db/consistency_level_type.hh"
#include "locator/tablets.hh"
#include "service/topology_guard.hh"

using namespace seastar;

namespace replica {
class database;
}

struct minimal_sst_info;
struct restore_result {
};

/// One node's upload-directory work for a single tablet, as measured by PREPARE_UPLOAD.
/// shards are the node's shards holding sstables which overlap the tablet - an sstable can
/// only be read by the shard which opened it, so the set travels with the work.
struct upload_work_item {
    uint64_t tablet_id = 0;
    std::vector<uint32_t> shards;
    // Bytes each entry of shards contributes, in the same order. Charging every shard the
    // tablet total would throttle a load by the shard count.
    std::vector<uint64_t> shard_bytes;
    // Estimate; see sstable::estimated_data_size_for_range().
    uint64_t estimated_bytes = 0;
};

struct upload_tablet_result {
};

struct finish_upload_result {
};

struct upload_replicate_result {
};

struct upload_stream_session_result {
};

struct prepare_upload_response {
    std::vector<upload_work_item> items;
    uint64_t total_estimated_bytes = 0;
    uint32_t sstable_count = 0;
};

namespace sstables { class storage_manager; enum class sstable_state; }

namespace netw { class messaging_service; }
namespace db {
class system_distributed_keyspace;
namespace view {
class view_builder;
class view_building_worker;
}
}
namespace service {
class storage_service;
}
namespace locator {
class effective_replication_map;
class tablet_metadata_guard;
}

struct stream_progress {
    float total = 0.;
    float completed = 0.;

    virtual ~stream_progress() = default;
    stream_progress& operator+=(const stream_progress& p) {
        total += p.total;
        completed += p.completed;
        return *this;
    }
    void start(float amount) {
        assert(amount >= 0);
        total = amount;
    }
    virtual void advance(float amount) {
        // we should not move backward
        assert(amount >= 0);
        completed += amount;
        // Clamp to total to absorb floating point rounding errors from
        // fractional progress contributions (e.g. per-tablet progress
        // computed as num_streamed / num_mapped).
        completed = std::min(completed, total);
    }
};

// The handler of the 'storage_service/load_new_ss_tables' endpoint which, in
// turn, is the target of the 'nodetool refresh' command.
// Gets sstables from the upload directory and makes them available in the
// system. Built on top of the distributed_loader functionality.
class sstables_loader : public seastar::peering_sharded_service<sstables_loader> {
public:
    enum class stream_scope { all, dc, rack, node };
    class task_manager_module : public tasks::task_manager::module {
        public:
            task_manager_module(tasks::task_manager& tm) noexcept : tasks::task_manager::module(tm, "sstables_loader") {}
    };

private:
    sharded<replica::database>& _db;
    sharded<service::storage_service>& _ss;
    netw::messaging_service& _messaging;
    sharded<db::view::view_builder>& _view_builder;
    sharded<db::view::view_building_worker>& _view_building_worker;
    shared_ptr<task_manager_module> _task_manager_module;
    sstables::storage_manager& _storage_manager;
    db::system_distributed_keyspace& _sys_dist_ks;
    seastar::scheduling_group _sched_group;

    // Note that this is obviously only valid for the current shard. Users of
    // this facility should elect a shard to be the coordinator based on any
    // given objective criteria
    //
    // It shouldn't be impossible to actively serialize two callers if the need
    // ever arise.
    bool _loading_new_sstables = false;

    // Serializes upload-directory scans, which open the whole directory. Only meaningful on
    // shard 0, which prepare_upload() routes to.
    seastar::semaphore _prepare_upload_sem{1};

    // Upload-directory sstables opened for one in-flight request; each shard holds what it
    // opened. Once per request, not per transition - a request makes one transition per tablet.
    struct upload_session {
        ::table_id table;
        // Sorted by first key so classification can stop early. Never mutated: several
        // transitions iterate it at once.
        std::vector<sstables::shared_sstable> sstables;
        // An sstable can be dropped once every tablet it overlaps is in here.
        std::unordered_set<uint64_t> completed_tablets;
        // So a second transition does not unlink again. Separate from the vector above, which
        // others may be iterating.
        std::unordered_set<const sstables::sstable*> unlinked;
        // Boundary-straddling sstables this shard is done with, for teardown to remove.
        // Deliberately not mark_for_deletion(): that unlinks on the last reference drop, so an
        // abort or shutdown would take every not-yet-streamed tablet's data with it.
        std::unordered_set<const sstables::sstable*> streamed;
        // Moved out of the upload directory by a local attach. A retried transition must not
        // pick them up: that reopens a missing TOC, which aborts the node.
        std::unordered_set<const sstables::sstable*> consumed;
        // The object which did the move lives on the shard that opened it, so the owning shard
        // is handed this instead.
        struct moved_sstable {
            sstables::generation_type generation;
            sstables::sstable_version_types version;
            sstables::sstable_format_types format;
            sstables::sstable_state state;
            // Attaches it, and so can tell whether it reached the table's sstable set.
            shard_id owning_shard;
        };
        // Attach not completed, by tablet: the file is in neither the upload directory nor the
        // table, so a retry resumes from here. Dropped once its sstable is in the table.
        std::unordered_map<uint64_t, std::vector<moved_sstable>> pending_attach;
        // Attached staging sstables whose view-building registration is still owed. Dropping
        // the record would report success with the view never built; a retry re-finds them by
        // generation.
        std::unordered_map<uint64_t, std::vector<moved_sstable>> pending_register;
        // Held while an attach has files out of the upload directory but not yet recorded as
        // attached. finish_upload() closes it before reading the two maps above, so what it
        // finds is quiescent and cannot be an sstable already in the table's sstable set.
        seastar::named_gate attach_gate{"upload_attach"};
    };
    std::unordered_map<utils::UUID, lw_shared_ptr<upload_session>> _upload_sessions;
    // Unpublished by finish_upload() but not yet torn down. Unpublishing is serialised against
    // ensure_upload_session() under _prepare_upload_sem; the teardown must not be, since it
    // waits on attach_gate and prepare_upload() takes that same semaphore.
    std::unordered_map<utils::UUID, lw_shared_ptr<upload_session>> _draining_upload_sessions;

    future<> load_and_stream(sstring ks_name, sstring cf_name,
            table_id, std::vector<sstables::shared_sstable> sstables,
            bool_class<struct primary_replica_only_tag> primary_replica_only, bool unlink_sstables, stream_scope scope,
            shared_ptr<stream_progress> progress);

    future<seastar::shared_ptr<const locator::effective_replication_map>> await_topology_quiesced_and_get_erm(table_id table_id);
    future<seastar::shared_ptr<const locator::effective_replication_map>> await_local_tablet_map_caught_up(table_id table_id);

    // One shard's contribution to the PREPARE_UPLOAD scan, keyed by tablet id.
    struct upload_measurement {
        std::map<uint64_t, upload_work_item> items;
        uint32_t sstable_count = 0;
        // Read per shard; the caller checks every shard saw the same one.
        int64_t topology_version = 0;
    };
    // Must run on the shard whose sstables_manager opened these sstables.
    future<upload_measurement> measure_upload_slice(table_id, std::vector<sstables::shared_sstable> ssts);
    future<prepare_upload_response> prepare_upload(table_id);
    future<> ensure_upload_session(utils::UUID request_id, table_id);
    future<upload_tablet_result> do_upload_tablet(locator::global_tablet_id, std::vector<uint32_t> source_shards);
    future<> stream_tablet_from_upload_dir(locator::global_tablet_id);
    future<> finish_upload(utils::UUID request_id, bool unlink_consumed);
    future<upload_replicate_result> do_upload_replicate_tablet(locator::global_tablet_id, shard_id dst_shard);
    // Returns how many were taken, so the caller streams only the rest.
    future<size_t> attach_local_upload_sstables(locator::global_tablet_id, shard_id owning_shard,
            std::vector<sstables::shared_sstable>& fully_contained, upload_session& session);
    // Only part of each input belongs to the tablet, so unlike a fully contained sstable they
    // cannot simply be moved - but nor need they go on the wire when this node is the primary.
    // Returns how many inputs it consumed. The rewrite cannot be interrupted, so as only decides
    // whether to start one and whether to attach a finished one.
    future<size_t> combine_local_upload_sstables(locator::global_tablet_id, shard_id owning_shard,
            const dht::token_range& tablet_range,
            std::vector<sstables::shared_sstable>& partially_contained, upload_session& session);
    future<> download_tablet_sstables(locator::global_tablet_id tid, locator::tablet_metadata_guard&);
    future<sstables::shared_sstable> attach_sstable(table_id tid, const minimal_sst_info& min_info) const;

public:
    sstables_loader(sharded<replica::database>& db,
            sharded<service::storage_service>& ss,
            netw::messaging_service& messaging,
            sharded<db::view::view_builder>& vb,
            sharded<db::view::view_building_worker>& vbw,
            tasks::task_manager& tm,
            sstables::storage_manager& sstm,
            db::system_distributed_keyspace& sys_dist_ks,
            seastar::scheduling_group sg);

    future<> stop();

    /**
     * Load new SSTables not currently tracked by the system
     *
     * This can be called, for instance, after copying a batch of SSTables to a CF directory.
     *
     * This should not be called in parallel for the same keyspace / column family, and doing
     * so will throw an std::runtime_exception.
     *
     * @param ks_name the keyspace in which to search for new SSTables.
     * @param cf_name the column family in which to search for new SSTables.
     * @param load_and_stream load SSTables that do not belong to this node and stream them to the appropriate nodes.
     * @param primary_replica_only whether to stream only to the primary replica that owns the data.
     * @param skip_cleanup whether to skip the cleanup step when loading SSTables.
     * @param skip_reshape whether to skip the reshape step when loading SSTables.
     * @return a future<> when the operation finishes.
     */
    future<> load_new_sstables(sstring ks_name, sstring cf_name,
            bool load_and_stream, bool primary_replica_only, bool skip_cleanup, bool skip_reshape, stream_scope scope);

    /**
     * Download new SSTables not currently tracked by the system from object store
     */
    future<tasks::task_id> download_new_sstables(sstring ks_name, sstring cf_name,
            sstring prefix, std::vector<sstring> sstables,
            sstring endpoint, sstring bucket, stream_scope scope, bool primary_replica);

    future<tasks::task_id> restore_tablets(table_id, sstring keyspace, sstring table, sstring snap_name, sstring endpoint, sstring bucket, sstring prefix, utils::chunked_vector<sstring> manifests);

    future<sstables::sstable_state> upload_destination_state(replica::table&);

    replica::database& local_db() {
        return _db.local();
    }

    class download_task_impl;
    class tablet_restore_task_impl;
};

template <>
struct fmt::formatter<sstables_loader::stream_scope> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables_loader::stream_scope a, FormatContext& ctx) const {
        using enum sstables_loader::stream_scope;
        switch (a) {
        case all:
            return formatter<string_view>::format("all", ctx);
        case dc:
            return formatter<string_view>::format("dc", ctx);
        case rack:
            return formatter<string_view>::format("rack", ctx);
        case node:
            return formatter<string_view>::format("node", ctx);
        }
    }
};

struct tablet_sstable_collection {
    dht::token_range tablet_range;
    std::vector<sstables::shared_sstable> sstables_fully_contained;
    std::vector<sstables::shared_sstable> sstables_partially_contained;
};

// This function is intended for test purposes only.
// It assigns the given sstables to the given tablet ranges based on token containment.
// It returns a vector of tablet_sstable_collection, each containing the tablet range
// and the sstables that are fully or partially contained within that range.
// The prerequisite is the tablet ranges are sorted by the range in ascending order and non-overlapping.
// Another prerequisite is that the sstables' token ranges are sorted by its `start` in descending order.
future<std::vector<tablet_sstable_collection>> get_sstables_for_tablets_for_tests(const std::vector<sstables::shared_sstable>& sstables,
                                                                                  std::vector<dht::token_range>&& tablets_ranges);

struct manifest_summary {
    size_t tablet_count;
    size_t nr_sstables;
};

future<manifest_summary> populate_snapshot_sstables_from_manifests(sstables::storage_manager& sm, db::system_distributed_keyspace& sys_dist_ks, sstring keyspace, sstring table, sstring endpoint, sstring bucket, sstring prefix, sstring expected_snapshot_name, utils::chunked_vector<sstring> manifest_prefixes, db::consistency_level cl = db::consistency_level::EACH_QUORUM);
