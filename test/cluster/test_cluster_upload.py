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
