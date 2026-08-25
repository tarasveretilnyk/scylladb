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
from test.pylib.rest_client import read_barrier
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
