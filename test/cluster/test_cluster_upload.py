#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for cluster-wide upload (nodetool cluster upload), which consumes every node's
upload directory in one operation scheduled by the tablet load balancer.

The point of the feature is not just that the data arrives. Node-local load-and-stream
already manages that. It is that tablets fill evenly and that tablet migrations are not
blocked while the load runs, so those are what these tests assert.
"""

import asyncio
import logging
import os
import shutil
import time
import uuid

import pytest
from cassandra.cluster import ConsistencyLevel

from test.cluster.util import (get_topology_coordinator, new_test_keyspace,
                               trigger_snapshot, trigger_stepdown)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.tablets import get_base_table
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

KEYS = 2048


async def populate(cql, ks, cf):
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
    insert.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(KEYS)))


async def cf_dir_of(manager, server, ks, cf='cf'):
    """Directory of one table, selected by name.

    A keyspace holds one directory per table, and a table with a materialized view has more
    than one - so taking the first entry picks the view's directory about half the time,
    depending on readdir order. That made the view test pass or fail by luck.
    """
    workdir = await manager.server_get_workdir(server.server_id)
    ks_dir = f'{workdir}/data/{ks}'
    matches = [d for d in os.listdir(ks_dir) if d == cf or d.startswith(f'{cf}-')]
    assert matches, f"no directory for table {cf} in {ks_dir}: {os.listdir(ks_dir)}"
    return os.path.join(ks_dir, matches[0])


async def snapshot_to_tmp(manager, servers, ks, cf, tmpname):
    """Snapshots the table on every node and copies the sstables aside, so that they can
    later be planted in upload directories."""
    await asyncio.gather(*(manager.api.take_snapshot(s.ip_addr, ks, tmpname) for s in servers))

    saved = {}
    for s in servers:
        cf_dir = await cf_dir_of(manager, s, ks, cf)
        snap_dir = os.path.join(cf_dir, 'snapshots', tmpname)
        tmp = os.path.join(cf_dir, f'../../../{tmpname}-{s.server_id}')
        os.makedirs(tmp, exist_ok=True)
        for item in os.listdir(snap_dir):
            if item not in ('manifest.json', 'schema.cql'):
                shutil.copy2(os.path.join(snap_dir, item), os.path.join(tmp, item))
        saved[s.server_id] = (cf_dir, tmp)
    return saved


async def plant_upload_dirs(saved):
    """Puts every node's saved sstables into its own upload directory."""
    for cf_dir, tmp in saved.values():
        upload = os.path.join(cf_dir, 'upload')
        os.makedirs(upload, exist_ok=True)
        shutil.copytree(tmp, upload, dirs_exist_ok=True)


async def wait_for_upload_dirs_empty(saved, timeout=60):
    """Every node's upload directory must be emptied once the request completes.

    nodetool refresh removes the sstables it consumed, so cluster upload has to as well -
    leaving them behind would both waste the space and re-ingest them on the next call.
    Removal is asynchronous, hence the wait.
    """
    async def check():
        leftover = {}
        for cf_dir, _ in saved.values():
            upload = os.path.join(cf_dir, 'upload')
            if os.path.isdir(upload):
                files = [f for f in os.listdir(upload) if not f.startswith('.')]
                if files:
                    leftover[upload] = files
        if leftover:
            return None
        return True
    await wait_for(check, time.time() + timeout, period=0.5)


async def upload_dir_files(saved):
    """Sstable files still sitting in every node's upload directory, by directory."""
    present = {}
    for cf_dir, _ in saved.values():
        upload = os.path.join(cf_dir, 'upload')
        if os.path.isdir(upload):
            files = [f for f in os.listdir(upload) if not f.startswith('.')]
            if files:
                present[upload] = files
    return present


async def table_id_of(cql, ks, cf):
    """CQL has no subqueries, so the table id has to be looked up separately."""
    rows = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE "
                               f"keyspace_name = '{ks}' AND table_name = '{cf}'")
    return rows[0].id


async def tablet_count(manager, cql, ks, cf):
    await read_barrier(manager.api, (await manager.running_servers())[0].ip_addr)
    tid = await table_id_of(cql, ks, cf)
    rows = await cql.run_async(f"SELECT tablet_count FROM system.tablets WHERE table_id = {tid}")
    return rows[0].tablet_count if rows else None


@pytest.mark.asyncio
async def test_cluster_upload_loads_all_data(manager: ScyllaClusterManager):
    """A single cluster upload call consumes every node's upload directory.

    This is the difference from nodetool refresh, which only consumes the directory of
    the node it is called on: here the data is planted on all nodes but the request is
    issued once.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)

        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        assert await cql.run_async(f"SELECT count(*) FROM {ks}.{cf}") == [(0,)]

        await plant_upload_dirs(saved)

        # Issued on one node only - the coordinator scans every node's directory.
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120)

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_uses_tablet_transitions(manager: ScyllaClusterManager):
    """Upload must go through per-tablet transitions, not a table-wide operation.

    That is the mechanism the whole feature rests on, so it is pinned directly: while the load
    runs, system.tablets shows rows in the 'upload' stage. Transitions are short-lived in dev
    mode, so the first ones are held open at the executor until such a row has been seen;
    sampling alone used to miss them, and the test then only logged a warning, pinning nothing.

    Note what this does NOT assert. The headline property - that tablets fill evenly rather
    than in token order - needs per-tablet sizes sampled during the load, and there is no
    observability for that yet. Adding the scheduler's counters (bytes remaining per node,
    transitions in flight) is what makes that assertion writable; until then this test only
    confirms the mechanism, not its effect.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        injection = "upload_tablet_before_transport"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)

        tid = await table_id_of(cql, ks, cf)
        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180))

        releaser = None
        try:
            async def upload_stage_seen():
                rows = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id = {tid}")
                return True if any(r.stage == 'upload' for r in rows) else None
            await wait_for(upload_stage_seen, time.time() + 60, period=0.2)

            async def release():
                while True:
                    for s in servers:
                        try:
                            await manager.api.message_injection(s.ip_addr, injection)
                        except Exception:
                            pass
                    await asyncio.sleep(0.05)

            releaser = asyncio.create_task(release())
            await upload
        finally:
            if releaser:
                releaser.cancel()
            for s in servers:
                try:
                    await manager.api.disable_injection(s.ip_addr, injection)
                except Exception:
                    pass

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}


@pytest.mark.asyncio
async def test_cluster_upload_and_node_join_complete_together(manager: ScyllaClusterManager):
    """A node join started during an upload and the upload must both finish.

    Node-local load-and-stream pins the table's effective_replication_map for its whole
    duration, which blocks every tablet transition in the cluster behind it, so a join started
    during a load waited for the load to end. Upload holds a guard scoped to one tablet instead.

    What this checks is liveness only: the join is started while the load runs, both complete,
    and the new node ends up owning tablets. It does not prove the two overlapped in time - a
    load this small finishes before a node can join, and holding transitions open to widen the
    window blocks the barriers the join itself needs. The next test pins the property that
    makes overlap harmless, the separation of the two load budgets, through the balancer's
    counters instead.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120))

        # Adding a node needs migrations, whose global barrier a table-wide ERM pin would block.
        new_server = await manager.server_add(config=BALANCER_CFG)
        await upload

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        tid = await table_id_of(cql, ks, cf)
        host_id = await manager.get_host_id(new_server.server_id)

        async def new_node_owns_tablets():
            rows = await cql.run_async(
                f"SELECT replicas FROM system.tablets WHERE table_id = {tid}")
            for row in rows:
                for replica in (row.replicas or []):
                    if str(replica[0]) == str(host_id):
                        return True
            return None

        # wait_for() compares against time.time(), so the deadline has to share that epoch.
        await wait_for(new_node_owns_tablets, time.time() + 90, period=1)


@pytest.mark.asyncio
async def test_cluster_upload_load_is_not_charged_to_the_migration_budget(manager: ScyllaClusterManager):
    """Upload load must be accounted apart from the load migrations are admitted against.

    Upload transitions are streaming work, so the balancer sees them. If they land in the
    same per-shard counters that can_accept_load() checks, a shard taking part in a load sits
    an order of magnitude above tablet_streaming_write_concurrency_per_shard (upload runs up
    to tablet_upload_concurrency_per_shard at once against a byte budget) and the balancer
    refuses to move tablets to or from it. Nothing fails - the data still arrives - the
    cluster just stops balancing for the duration, which is the interference this feature
    exists to remove.

    Asserted through the balancer's own node-stats line rather than end to end. Racing a node
    join against a load is not reproducible in either direction: a small load finishes before
    a node can join, and holding transitions open to widen the window blocks the barriers the
    join itself needs. The counters are the thing that was wrong, so the counters are what
    this checks - up_* moving while rd=/wr= stay at zero is precisely what keeps migrations
    admissible.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 32}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await lg.mark() for lg in logs]

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=150)

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        # Upload load must land in the upload fields, not on rd=/wr= and the migration budget.
        upload_accounted = []
        migration_charged = []
        for lg, mk in zip(logs, marks):
            upload_accounted += await lg.grep(r"up_rd=[1-9]|up_wr=[1-9]", from_mark=mk)
            migration_charged += await lg.grep(r" rd=[1-9]| wr=[1-9]", from_mark=mk)

        assert upload_accounted, \
            "the balancer never reported any upload load; either it was charged to the " \
            "migration counters instead, or no transition was in flight when it looked"
        assert not migration_charged, \
            f"{len(migration_charged)} node-stats line(s) charged streaming load to the " \
            f"migration counters during an upload with no migrations running: " \
            f"{migration_charged[0][0] if migration_charged else ''}"


@pytest.mark.asyncio
async def test_cluster_upload_fails_when_a_source_node_is_removed(manager: ManagerClient):
    """A source node leaving with work outstanding must fail the request, naming the node.

    Its share of the upload directory is gone with it, so the load cannot be completed and
    reporting success would claim data that was never ingested. The distinction the scheduler
    draws is between a node that has left the topology - unrecoverable, fail and say which -
    and one that is merely not in normal state, whose work is held back for a later pass
    because it may well come back. This covers the first; the second needs a node parked
    mid-decommission and is not reproducible from here.

    The transitions are parked so the request cannot drain before the node is removed.
    """
    servers = await manager.servers_add(3, config=BALANCER_CFG)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        victim = servers[2]
        victim_host = await manager.get_host_id(victim.server_id)

        injection = "upload_tablet_before_transport"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)

        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=240))

        async def work_exists():
            rows = await cql.run_async("SELECT tablet_id FROM system.upload_work")
            return True if rows else None

        error = None
        try:
            await wait_for(work_exists, time.time() + 60, period=0.1)

            # Killed rather than stopped gracefully: it is parked inside the injection, so a
            # graceful shutdown has nothing to drain into and would abort on the shutdown
            # timeout. Its work stays outstanding because it is no longer there to stream it.
            await manager.server_stop(victim.server_id, convict=True)

            # The survivors have to be let go before the node is removed. Their parked
            # transitions each hold a session guard, and removenode waits on a barrier that
            # those guards block - the operation would hang rather than proceed.
            for s in servers[:2]:
                await manager.api.disable_injection(s.ip_addr, injection)
                try:
                    await manager.api.message_injection(s.ip_addr, injection)
                except Exception:
                    pass

            await manager.remove_node(servers[0].server_id, victim.server_id)

            try:
                await upload
            except Exception as e:
                error = str(e)
        finally:
            for s in servers[:2]:
                try:
                    await manager.api.disable_injection(s.ip_addr, injection)
                except Exception:
                    pass

        assert error is not None, ("the request completed although a source node left with work "
                                   "outstanding, so it reported success for data it never loaded")
        assert str(victim_host) in error, \
            f"the failure does not name the node that left, so an operator cannot tell which: {error}"

        # A failed request must not wedge the table: a second upload has to be accepted and
        # complete, rather than joining the failed request and inheriting its outcome.
        #
        # Deliberately not asserting that the survivors' upload directories still hold files.
        # At RF=1 with files planted into the same tablet count they are all fully contained,
        # so each survivor legitimately consumes its own before the request fails - there is
        # nothing left to preserve. The straddling case, where a failure must not delete what
        # it was still re-sending, is covered by test_cluster_upload_abort, which parks every
        # transition before any transport runs and so can assert the directories untouched.
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=240)


@pytest.mark.asyncio
async def test_cluster_upload_rejects_vnode_table(manager: ScyllaClusterManager):
    """Cluster upload is tablet-only; a vnode table has to be rejected rather than
    silently doing nothing."""
    servers = await manager.servers_add(1)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int)")
        with pytest.raises(HTTPError, match="does not use tablets"):
            await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120)


@pytest.mark.asyncio
async def test_cluster_upload_rejects_colocated_table(manager: ScyllaClusterManager):
    """A co-located table has to be rejected up front, like a vnode table.

    Its tablet map is the base table's, so it has no system.tablets partition of its own for
    the coordinator to write upload transitions into. Such a request used to be accepted: the
    coordinator then produced the same rejected mutation every pass and every other tablet
    operation waited behind it until someone aborted the upload.
    """
    servers = await manager.servers_add(1, config=BALANCER_CFG)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 4}}")
        # A view keyed by the base table's partition key is co-located with it.
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.{cf} "
                            f"WHERE pk IS NOT NULL AND value IS NOT NULL PRIMARY KEY (pk, value)")
        rows = await cql.run_async(f"SELECT id FROM system_schema.views WHERE "
                                   f"keyspace_name = '{ks}' AND view_name = 'mv'")
        mv_id = rows[0].id
        assert await get_base_table(manager, mv_id) != mv_id, \
            "the view is not co-located with its base table, so this test would prove nothing"

        with pytest.raises(HTTPError, match="co-located"):
            await manager.api.tablets_upload(servers[0].ip_addr, ks, 'mv', timeout=60)

        rows = await cql.run_async("SELECT upload_table_id FROM system.topology_requests ALLOW FILTERING")
        assert not [r for r in rows if getattr(r, 'upload_table_id', None) == mv_id], \
            "a request was created for the rejected table"

        # The base table itself is unaffected.
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120)


@pytest.mark.asyncio
async def test_cluster_upload_with_empty_upload_dirs(manager: ScyllaClusterManager):
    """A request with nothing to do must complete rather than hang.

    The coordinator short-circuits when every node reports an empty directory; without
    that the request would sit in the queue with no work to consume.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 4}}")
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120)


@pytest.mark.asyncio
async def test_cluster_upload_pre_sizes_table(manager: ScyllaClusterManager):
    """tablet_count resizes the table before any data is ingested.

    Loading into an under-provisioned table is what produces the oversized tablets the
    feature exists to avoid, so the resize has to happen first, not afterwards.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 2}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        before = await tablet_count(manager, cql, ks, cf)
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, tablet_count=16, timeout=120)
        after = await tablet_count(manager, cql, ks, cf)

        assert before is not None and after is not None
        assert after >= 16, f"table was not pre-sized: {before} -> {after}"

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}


@pytest.mark.asyncio
async def test_cluster_upload_pre_size_fails_fast_with_balancing_disabled(manager: ScyllaClusterManager):
    """--tablets must fail, not hang, when the balancer will never reach the count.

    The pre-size pins min == max and waits for the balancer to get the table there. With
    balancing disabled make_resize_plan() returns before it decides anything, so that wait
    used to be indefinite, with no abort source to cut it short: the task could only be got
    rid of with the node. It has to fail as soon as it sees the balancer cannot act, and put
    back the hints it pinned.
    """
    servers = await manager.servers_add(1, config=BALANCER_CFG)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 4}}")

        with pytest.raises(RuntimeError, match="balancing is disabled"):
            await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, tablet_count=16, timeout=60)

        # A table with no max hint of its own must not be left with this task's pin.
        rows = await cql.run_async(f"SELECT tablets FROM system_schema.scylla_tables WHERE "
                                   f"keyspace_name = '{ks}' AND table_name = '{cf}'")
        opts = dict(rows[0].tablets or {})
        assert 'max_tablet_count' not in opts, f"the failed pre-size left its pin in place: {opts}"
        assert opts.get('min_tablet_count') == '4', f"the original hint was not put back: {opts}"


@pytest.mark.asyncio
async def test_cluster_upload_work_drains_across_tablets(manager: ScyllaClusterManager):
    """Several tablets must be uploading at once, not one after another.

    This is the property the ticket is about: the old loader worked one tablet at a time,
    so the cluster sat idle and early tablets filled while later ones stayed empty.

    Sampling for it while the load runs does not work - on a dev-mode cluster with a small
    dataset the whole upload finishes inside one sampling interval, so an earlier version
    of this test passed or failed on timing luck. Instead every transition is pinned at an
    error injection; while they are held, system.tablets can be read without racing
    anything.

    The injection has to sit in the upload path itself rather than in the mutation
    streaming path: an upload moves a fully contained sstable by direct attach or by file
    stream, and only a boundary-straddling one goes through mutations. Pinning the mutation
    path alone left this test measuring whichever tablets happened to need it.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        injection = "upload_tablet_before_transport"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)

        tid = await table_id_of(cql, ks, cf)
        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120))

        # Release the held batches in the background; without this the injection stalls it forever.
        async def release():
            while True:
                for s in servers:
                    try:
                        await manager.api.message_injection(s.ip_addr, injection)
                    except Exception:
                        pass
                await asyncio.sleep(0.05)

        peak = 0

        async def watch():
            nonlocal peak
            while True:
                try:
                    rows = await cql.run_async(
                        f"SELECT stage FROM system.tablets WHERE table_id = {tid}")
                    peak = max(peak, sum(1 for r in rows if r.stage == 'upload'))
                except Exception:
                    pass
                await asyncio.sleep(0.05)

        watcher = asyncio.create_task(watch())

        # If more than one transition is held at once the load is not serialised - the whole point.
        async def more_than_one_held():
            return True if peak > 1 else None
        try:
            await wait_for(more_than_one_held, time.time() + 30, period=0.1)
        except Exception:
            logger.warning("never saw more than one held transition; the assertion below will say so")

        releaser = asyncio.create_task(release())

        try:
            await upload
        finally:
            watcher.cancel()
            releaser.cancel()
            for s in servers:
                try:
                    await manager.api.disable_injection(s.ip_addr, injection)
                except Exception:
                    pass

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        logger.info(f"peak concurrent upload transitions: {peak}")
        assert peak > 1, (f"only {peak} tablet(s) were ever uploading at once; the load is "
                          f"still serialised one tablet at a time")


@pytest.mark.asyncio
async def test_cluster_upload_abort(manager: ScyllaClusterManager):
    """An upload must be cancellable, and cancelling must not lose the unconsumed data.

    Three things have to hold, and this test used to check none of them: the request has to
    report the abort as its outcome rather than success, the sstables it did not consume have
    to still be in the upload directories, and a later upload of the same table has to
    actually re-ingest them instead of joining the aborted request and inheriting its result.

    Every transition is parked before it picks a transport, so the abort lands while the
    request is live and provably before anything has been consumed. Without that the upload
    can finish before the abort arrives, which is what made the old version of this test
    accept either outcome - and therefore assert nothing.
    """
    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)
        planted = await upload_dir_files(saved)
        assert planted, "nothing was planted, so the rest of this test would prove nothing"

        injection = "upload_tablet_before_transport"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)

        tid = await table_id_of(cql, ks, cf)
        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180))

        async def transitions_parked():
            rows = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id = {tid}")
            return True if sum(1 for r in rows if r.stage == 'upload') > 0 else None

        releaser = None
        try:
            await wait_for(transitions_parked, time.time() + 60, period=0.1)
            await manager.api.tablets_upload_abort(servers[0].ip_addr, ks, cf, timeout=60)

            async def release():
                while True:
                    for s in servers:
                        try:
                            await manager.api.message_injection(s.ip_addr, injection)
                        except Exception:
                            pass
                    await asyncio.sleep(0.05)

            releaser = asyncio.create_task(release())

            # The abort is the outcome: success would let teardown delete sstables never loaded.
            with pytest.raises(Exception):
                await upload
        finally:
            if releaser:
                releaser.cancel()
            for s in servers:
                await manager.api.disable_injection(s.ip_addr, injection)

        async def work_gone():
            rows = await cql.run_async("SELECT tablet_id FROM system.upload_work")
            return True if not rows else None
        await wait_for(work_gone, time.time() + 60, period=0.5)

        left = await upload_dir_files(saved)
        assert set(left) == set(planted), (
            f"abort lost unconsumed sstables: planted {planted}, left {left}")
        assert await cql.run_async(f"SELECT count(*) FROM {ks}.{cf}") == [(0,)], \
            "rows were ingested by a request that was aborted before any transport ran"

        # A second upload must re-scan, not join the aborted request and inherit its outcome.
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180)
        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}
        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_abort_interrupts_streaming(manager: ScyllaClusterManager):
    """Aborting must stop a transfer that is already under way, not only stop new ones.

    The executor holds a session_topology_guard, and clearing the transition's session fires
    its abort source. That was inert for a while: the guard was constructed and the abort
    source never consulted, so a transfer already started ran to completion and the abort
    only took effect at the next transition boundary. It is now checked before each file and
    between chunks on the file path, and per fragment on the mutation path.

    The sibling abort test cannot show this - it accepts termination however it arrives. Here
    the transition is parked before it picks a transport, aborted while parked, and then
    released: the first check after the injection lets go has to fail it, so the abort has to
    appear in the log as a torn-down stream rather than as a tablet that simply finished.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    # RF=3 so phase 1 has somewhere to stream to; at RF=1 nothing goes on the wire to interrupt.
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        injection = "upload_tablet_before_transport"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=False)

        tid = await table_id_of(cql, ks, cf)
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await lg.mark() for lg in logs]

        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180))

        async def transitions_parked():
            rows = await cql.run_async(f"SELECT stage FROM system.tablets WHERE table_id = {tid}")
            return True if sum(1 for r in rows if r.stage == 'upload') > 0 else None

        releaser = None
        try:
            await wait_for(transitions_parked, time.time() + 60, period=0.1)

            # Abort while transfers are held: each session is closed, so its abort source has fired.
            await manager.api.tablets_upload_abort(servers[0].ip_addr, ks, cf, timeout=60)

            async def release():
                while True:
                    for s in servers:
                        try:
                            await manager.api.message_injection(s.ip_addr, injection)
                        except Exception:
                            pass
                    await asyncio.sleep(0.05)

            releaser = asyncio.create_task(release())

            try:
                await upload
            except Exception as e:
                logger.info(f"upload ended with {e!r} after abort, which is expected")

            aborted = []
            for lg, mk in zip(logs, marks):
                aborted += await lg.grep(r"Upload of tablet .* was aborted|"
                                         r"abort requested|"
                                         r"Master failed sending file|"
                                         r"send_phase, err", from_mark=mk)
            assert aborted, ("no transfer reported being interrupted; the abort took effect "
                             "only between transitions, so the abort source is not being checked")
        finally:
            if releaser:
                releaser.cancel()
            for s in servers:
                try:
                    await manager.api.disable_injection(s.ip_addr, injection)
                except Exception:
                    pass


async def view_row_count(cql, ks, view):
    rows = await cql.run_async(f"SELECT COUNT(*) AS c FROM {ks}.{view}")
    return rows[0].c


async def three_rack_servers(manager):
    """RF=3 requires three racks, since a replica set must span them."""
    return await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])


@pytest.mark.asyncio
async def test_cluster_upload_replicates_to_all_replicas(manager: ScyllaClusterManager):
    """At RF>1 the data must end up on every replica, not just the primary.

    Phase 1 streams into one replica only - the primary the scheduler assigned - and phase 2
    file-streams it from there to the rest. Every other test in this file runs at RF=1,
    where the primary is the only replica and phase 2 never runs, so this is the first test
    that exercises it at all.

    Reading at ALL does NOT prove this on its own: the consistency level controls how many
    replicas must respond, not that each holds the data, so a read would still return every
    row if only the primary had it. The proof is instead taken two ways - the coordinator log
    must show replicate transitions being driven, and every tablet must have left the
    replicating phase.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        # Watch the coordinator log so replication is shown, not inferred from a successful read.
        coord_log = await manager.server_open_log(servers[0].server_id)
        log_mark = await coord_log.mark()

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180)

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}

        replicated = await coord_log.grep("Replicating uploaded tablet", from_mark=log_mark)
        assert replicated, ("no replicate transition was driven, so phase 2 never ran and the "
                            "data may be on primaries only")

        leftover = await cql.run_async("SELECT tablet_id FROM system.upload_tablet_state")
        assert not leftover, f"{len(leftover)} tablets still have upload state after completion"

        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_retries_failed_replication(manager: ScyllaClusterManager):
    """A failed phase-2 replication must be retried until the data reaches every replica.

    Phase 2 failing clears the transition but leaves the tablet in the replicating phase, so
    the scheduler picks it up again after its backoff. That retry had never run: the backoff
    it depends on lived on the load balancer, which is rebuilt for every scheduling pass, so
    the state was always empty and nothing was ever held back or resumed. Nothing in the
    system provokes a phase-2 failure on its own either, hence the injection.

    A read at ALL would not show this - the consistency level says how many replicas must
    answer, not that each holds the data - so the proof is that every tablet leaves the
    replicating phase, which only happens once its replication has actually succeeded.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        # One shot per node: each fails its first replication, so the load must recover.
        injection = "upload_replicate_fail"
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, injection, one_shot=True)

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await lg.mark() for lg in logs]

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=240)

        failed = []
        for lg, mk in zip(logs, marks):
            failed += await lg.grep("Injected upload_replicate failure", from_mark=mk)
        assert failed, "the injection never fired; the retry path was not exercised"

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        leftover = await cql.run_async("SELECT tablet_id FROM system.upload_tablet_state")
        assert not leftover, f"{len(leftover)} tablets still replicating after completion"

        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_primary_replica_only(manager: ScyllaClusterManager):
    """--primary-replica-only stops after phase 1.

    The data must land, but replication must not happen - that is the whole point of the
    mode, and it is the negation of the previous test.

    Consistency level cannot express that. A read at ONE picks one replica and would find
    nothing whenever it picks a replica that never received the data; a read at ALL merges
    across replicas and returns every row as long as any one of them has it. So the mode is
    verified by reading at ALL for the data and by checking the coordinator log for the
    absence of replicate transitions.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        coord_log = await manager.server_open_log(servers[0].server_id)
        log_mark = await coord_log.mark()

        # Must be accepted now that phase 2 exists; it used to be rejected outright.
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf,
                                                  primary_replica_only=True, timeout=180)

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}

        replicated = await coord_log.grep("Replicating uploaded tablet", from_mark=log_mark)
        assert not replicated, ("phase 2 ran even though primary_replica_only was requested, so "
                                "the data was replicated when it should not have been")

        leftover = await cql.run_async("SELECT tablet_id FROM system.upload_tablet_state")
        assert not leftover, "request finished with upload state left behind"


@pytest.mark.asyncio
async def test_cluster_upload_builds_views(manager: ScyllaClusterManager):
    """Views must be populated on every replica after an upload.

    This is the path I had reasoned about wrongly: view updates are generated per base
    replica for its paired view replica, so an sstable that arrives by file streaming has to
    carry staging state or the replicas that never streamed would never populate their view
    replicas. Both new transport paths - local attach and remote file streaming - decide
    that state themselves, so this covers both.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text, value int, primary key (pk))")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.{cf} "
                            f"WHERE value IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (value, pk)")
        await populate(cql, ks, cf)

        async def view_ready():
            return True if await view_row_count(cql, ks, 'mv') == KEYS else None
        await wait_for(view_ready, time.time() + 60, period=0.5)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")

        async def view_empty():
            return True if await view_row_count(cql, ks, 'mv') == 0 else None
        await wait_for(view_empty, time.time() + 60, period=0.5)

        await plant_upload_dirs(saved)
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=120)

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}

        async def view_rebuilt():
            n = await view_row_count(cql, ks, 'mv')
            return True if n == KEYS else None
        try:
            await wait_for(view_rebuilt, time.time() + 90, period=1)
        except Exception:
            n = await view_row_count(cql, ks, 'mv')
            raise AssertionError(f"view has {n} rows, expected {KEYS}: uploaded sstables did not "
                                 f"generate view updates on all replicas")


async def multi_dc_servers(manager):
    """Two DCs with asymmetric replication, to exercise cross-DC source selection."""
    return await manager.servers_add(5, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
        {"dc": "dc2", "rack": "r1"},
        {"dc": "dc2", "rack": "r2"},
    ])


@pytest.mark.asyncio
async def test_cluster_upload_multi_dc_asymmetric_rf(manager: ScyllaClusterManager):
    """Five nodes, two DCs, RF 3 in one and 2 in the other.

    Every node holds sstables for the whole ring, so most work items have a source in a
    different rack or DC from the tablet's replicas. That is what exercises the locality
    gradation and the cross-DC fallback: a source in dc2 for a tablet whose replicas are in
    dc1 scores worse than a local one but must still be scheduled, or its share of the data
    is never loaded.
    """
    servers = await multi_dc_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'dc1': 3, 'dc2': 2}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        coord_log = await manager.server_open_log(servers[0].server_id)
        log_mark = await coord_log.mark()

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=150)

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}

        # Work from all five directories has to have been consumed, not just the local ones.
        await wait_for_upload_dirs_empty(saved, timeout=120)

        assert await coord_log.grep("Replicating uploaded tablet", from_mark=log_mark), \
            "no replication happened despite RF>1 in both datacenters"

        leftover = await cql.run_async("SELECT tablet_id FROM system.upload_tablet_state")
        assert not leftover, f"{len(leftover)} tablets left mid-phase"


@pytest.mark.asyncio
async def test_cluster_upload_uses_both_transport_paths(manager: ScyllaClusterManager):
    """Both ingest paths must actually be taken.

    A tablet whose primary is the node holding the sstables is attached in place, with no
    transfer at all; any other tablet is sent as files. With several nodes each holding data
    for the whole ring, both cases occur, and a green test that silently took only one of
    them would hide a broken path - which is how the file-streaming path stayed broken
    through several passing runs.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        # The per-tablet executor lines this test greps for are debug level.
        for s in servers:
            await manager.api.set_logger_level(s.ip_addr, 'sstables_loader', 'debug')
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await lg.mark() for lg in logs]

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=150)

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}

        local, streamed = 0, 0
        for lg, mk in zip(logs, marks):
            local += len(await lg.grep("Attached .* fully contained sstables locally", from_mark=mk))
            streamed += len(await lg.grep(r"upload_file_stream\[.*\] tablet .* sent", from_mark=mk))

        logger.info(f"local attaches: {local}, file streams completed: {streamed}")
        assert local > 0, "no sstable was attached locally; the same-host path never ran"
        assert streamed > 0, "no sstable was file-streamed; the remote path never ran"


@pytest.mark.asyncio
async def test_cluster_upload_combines_straddling_sstables_locally(manager: ScyllaClusterManager):
    """An sstable that crosses a tablet boundary must not be sent over the wire to this node.

    Only part of such an sstable belongs to the tablet, so unlike a fully contained one it
    cannot simply be moved out of the upload directory. That used to mean handing it to the
    ordinary streaming path with the target set to ourselves, which serialises every row,
    sends it over a loopback RPC and deserialises it again to arrive where it started. It is
    read-combined into a new sstable in place instead.

    RF=1 on a single node makes this node the primary for every tablet, so no transfer is
    ever warranted. The absence of the mutation path is what the test is really pinning - a
    regression there would still load the data correctly and only show up as cost.
    """
    server = await manager.server_add()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        # Flushing is tablet-aware, so the boundaries have to move underneath a file to straddle it.
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 2}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, [server], ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        # The per-tablet executor lines this test greps for are debug level.
        await manager.api.set_logger_level(server.ip_addr, 'sstables_loader', 'debug')
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.api.tablets_upload_and_wait(server.ip_addr, ks, cf, tablet_count=32, timeout=150)

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}

        straddling = await log.grep(r"partially contained sstables", from_mark=mark)
        combined = await log.grep(r"Combined .* boundary-straddling", from_mark=mark)
        looped = await log.grep(r"load_and_stream: started ops_uuid", from_mark=mark)

        # Guards the premise: without a straddling sstable the assertions below prove nothing.
        assert any(", 0 partially contained" not in line for line, _ in straddling), \
            "no sstable straddled a tablet boundary; the test proves nothing"
        assert combined, "a straddling sstable was not combined in place"
        assert not looped, \
            f"{len(looped)} mutation stream(s) were started although this node is the " \
            f"primary for every tablet; the loopback path is back"

        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_reports_metrics(manager: ScyllaClusterManager):
    """The scheduler's counters must move, since they are the only way to tell a slow upload
    from a stalled one in the field."""
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180)

        metrics = await manager.metrics.query(servers[0].ip_addr)
        produced = metrics.get('scylla_load_balancer_uploads_produced')
        assert produced is not None, "uploads_produced metric is missing"
        assert produced > 0, f"uploads_produced did not move: {produced}"

        remaining = metrics.get('scylla_load_balancer_upload_work_items_remaining')
        assert remaining == 0, f"upload_work_items_remaining is {remaining} after completion"


@pytest.mark.asyncio
async def test_cluster_upload_two_tables_concurrently(manager: ScyllaClusterManager):
    """Two tables uploading at once must not interfere.

    Their work rows share system.upload_work and their per-tablet state shares
    system.upload_tablet_state, both keyed by request, so a query that forgot to filter by
    request or table would mix them - and progress reporting did exactly that in an earlier
    revision.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        for cf in ('cf1', 'cf2'):
            await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                                f"WITH tablets = {{'min_tablet_count': 8}}")
            insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
            insert.consistency_level = ConsistencyLevel.ALL
            await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(KEYS)))

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved1 = await snapshot_to_tmp(manager, servers, ks, 'cf1', tmpname + '-1')
        saved2 = await snapshot_to_tmp(manager, servers, ks, 'cf2', tmpname + '-2')
        await cql.run_async(f"TRUNCATE TABLE {ks}.cf1")
        await cql.run_async(f"TRUNCATE TABLE {ks}.cf2")
        await plant_upload_dirs(saved1)
        await plant_upload_dirs(saved2)

        await asyncio.gather(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, 'cf1', timeout=150),
            manager.api.tablets_upload_and_wait(servers[1].ip_addr, ks, 'cf2', timeout=150))

        for cf in ('cf1', 'cf2'):
            stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
            stmt.consistency_level = ConsistencyLevel.ALL
            got = {row.pk for row in await cql.run_async(stmt)}
            assert got == {str(k) for k in range(KEYS)}, f"{cf} did not load fully"

        leftover = await cql.run_async("SELECT tablet_id FROM system.upload_work")
        assert not leftover, "work rows left behind after both requests completed"


@pytest.mark.asyncio
async def test_cluster_upload_rejects_second_request_for_same_table(manager: ScyllaClusterManager):
    """A second request for the same table must join the first rather than run twice.

    Two independent requests would consume the same upload directories concurrently and
    ingest the same sstables twice - idempotent for ordinary rows, wrong for counters.
    """
    servers = await manager.servers_add(2)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 1}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        # Both must succeed - the second joins the first - and the data must be loaded once.
        await asyncio.gather(
            manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=180),
            manager.api.tablets_upload_and_wait(servers[1].ip_addr, ks, cf, timeout=180))

        got = {row.pk for row in await cql.run_async(f"SELECT pk FROM {ks}.{cf}")}
        assert got == {str(k) for k in range(KEYS)}
        await wait_for_upload_dirs_empty(saved)


@pytest.mark.asyncio
async def test_cluster_upload_survives_coordinator_restart(manager: ScyllaClusterManager):
    """The topology coordinator can go away mid-upload and the load must still finish.

    This is the crash-safety property the request setup was designed around and which
    nothing has exercised: the work list is written across several group0 commands and the
    request is only marked started once all of it has landed, so a coordinator that dies
    midway should leave a queued-but-not-started request that a new coordinator clears and
    rescans. A coordinator that dies later, with transitions in flight, should instead find
    the work list intact and carry on from it.

    Restarting the raft leader is what moves the coordinator, so that is what this does.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 16}}")
        await populate(cql, ks, cf)

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")
        await plant_upload_dirs(saved)

        leader_host = await manager.api.get_raft_leader(servers[0].ip_addr)
        leader = await manager.find_server_by_host_id(servers, leader_host)
        others = [s for s in servers if s.server_id != leader.server_id]
        logger.info(f"coordinator is on {leader.server_id}, restarting it mid-upload")

        upload = asyncio.create_task(
            manager.api.tablets_upload_and_wait(others[0].ip_addr, ks, cf, timeout=150))

        async def work_exists():
            rows = await cql.run_async("SELECT tablet_id FROM system.upload_work")
            return True if rows else None
        try:
            await wait_for(work_exists, time.time() + 60, period=0.05)
        except Exception:
            logger.warning("upload finished before work was observed; restart will be a no-op")

        await manager.server_restart(leader.server_id, wait_others=2)

        # A new coordinator may resume or rebuild the work list, but must not drop part of it.
        await upload

        stmt = cql.prepare(f"SELECT pk FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        got = {row.pk for row in await cql.run_async(stmt)}
        assert got == {str(k) for k in range(KEYS)}, \
            f"{len(set(str(k) for k in range(KEYS)) - got)} rows lost across coordinator restart"

        leftover_work = await cql.run_async("SELECT tablet_id FROM system.upload_work")
        leftover_state = await cql.run_async("SELECT tablet_id FROM system.upload_tablet_state")
        assert not leftover_work, f"{len(leftover_work)} work rows left after restart"
        assert not leftover_state, f"{len(leftover_state)} tablet state rows left after restart"


@pytest.mark.asyncio
async def test_cluster_upload_into_non_empty_table(manager: ScyllaClusterManager):
    """Uploading on top of existing data must merge, not replace or corrupt.

    Every other test truncates first, so the ingested sstables are the only ones in the
    table. Here they land alongside existing sstables, which is what exercises the decision
    to mutate ingested sstables to level 0: keeping their original levels would break the
    compaction strategy's invariants against the data already there.
    """
    servers = await three_rack_servers(manager)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                                          "'replication_factor': 3}") as ks:
        cf = 'cf'
        await cql.run_async(f"CREATE TABLE {ks}.{cf} (pk text primary key, value int) "
                            f"WITH tablets = {{'min_tablet_count': 8}}")

        insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, value) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(KEYS // 2)))

        tmpname = f'up-{uuid.uuid4().hex[:8]}'
        saved = await snapshot_to_tmp(manager, servers, ks, cf, tmpname)
        await cql.run_async(f"TRUNCATE TABLE {ks}.{cf}")

        await asyncio.gather(*(cql.run_async(insert, (str(k), k)) for k in range(KEYS // 2, KEYS)))
        await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))

        await plant_upload_dirs(saved)
        await manager.api.tablets_upload_and_wait(servers[0].ip_addr, ks, cf, timeout=150)

        stmt = cql.prepare(f"SELECT pk, value FROM {ks}.{cf}")
        stmt.consistency_level = ConsistencyLevel.ALL
        rows = {row.pk: row.value for row in await cql.run_async(stmt)}

        assert set(rows) == {str(k) for k in range(KEYS)}, (
            f"expected both halves, missing {len(set(str(k) for k in range(KEYS)) - set(rows))}")
        for k in range(KEYS):
            assert rows[str(k)] == k, f"value for {k} became {rows[str(k)]} after the merge"

        await wait_for_upload_dirs_empty(saved)
