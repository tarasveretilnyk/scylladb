/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

class restore_result {
};

// One node's work for a tablet; shards travel with it - only the opening shard can read an sstable.
struct upload_work_item {
    uint64_t tablet_id;
    std::vector<uint32_t> shards;
    std::vector<uint64_t> shard_bytes;
    uint64_t estimated_bytes;
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
    uint64_t total_estimated_bytes;
    uint32_t sstable_count;
};

verb [[]] restore_tablet (raft::server_id dst_id, locator::global_tablet_id gid) -> restore_result;
verb [[with_timeout]] prepare_upload (raft::server_id dst_id, table_id table) -> prepare_upload_response;
verb [[]] upload_tablet (raft::server_id dst_id, locator::global_tablet_id gid, std::vector<uint32_t> source_shards) -> upload_tablet_result;
verb [[with_timeout]] finish_upload (raft::server_id dst_id, utils::UUID request_id, bool unlink_consumed) -> finish_upload_result;
verb [[]] upload_replicate_tablet (raft::server_id dst_id, locator::global_tablet_id gid, uint32_t dst_shard) -> upload_replicate_result;
verb [[]] upload_stream_session (raft::server_id dst_id, utils::UUID ops_id, bool start) -> upload_stream_session_result;
