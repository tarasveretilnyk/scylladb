#
# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for 'nodetool cluster upload'.

These cover the translation from command line to REST request, which nothing else does:
the cluster tests drive the REST endpoint directly, so an argument that is parsed wrongly
or a parameter that is never forwarded would not show up there.
"""

import pytest

from test.nodetool.utils import check_nodetool_fails_with_error_contains
from test.nodetool.rest_api_mock import expected_request

TASK_ID = "ef1b7a61-66c8-494c-bb03-6f65724e6eee"
START_TIME = "2026-08-25T10:00:00Z"
END_TIME = "2026-08-25T10:05:00Z"


def _keyspace_lookups(ks="ks1", tablets=True):
    """The requests every invocation makes before it can issue the upload itself."""
    return [
        expected_request("GET", "/storage_service/keyspaces", response=[ks]),
        expected_request("GET", "/storage_service/keyspaces",
                         params={"replication": "tablets"},
                         response=[ks] if tablets else []),
    ]


def _wait_task(state="done", error=""):
    """The command waits on the task it started unless --nowait was given."""
    return expected_request(
        "GET",
        f"/task_manager/wait_task/{TASK_ID}",
        response={"state": state, "error": error,
                  "start_time": START_TIME, "end_time": END_TIME})


def test_cluster_upload(nodetool):
    res = nodetool("cluster", "upload", "ks1", "table1", expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1"},
                         response=TASK_ID),
        _wait_task()])
    assert "done" in res.stdout


def test_cluster_upload_with_tablet_count(nodetool):
    nodetool("cluster", "upload", "ks1", "table1", "--tablets", "64",
             expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1", "tablet_count": "64"},
                         response=TASK_ID),
        _wait_task()])


def test_cluster_upload_primary_replica_only(nodetool):
    nodetool("cluster", "upload", "ks1", "table1", "--primary-replica-only",
             expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1", "primary_replica_only": "true"},
                         response=TASK_ID),
        _wait_task()])


def test_cluster_upload_nowait(nodetool):
    """--nowait hands back the task id instead of blocking, and issues no wait request."""
    res = nodetool("cluster", "upload", "ks1", "table1", "--nowait",
                   expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1"},
                         response=TASK_ID)])
    assert TASK_ID in res.stdout


def test_cluster_upload_reports_failure(nodetool):
    """A load that ends in any state other than done has to fail the command, otherwise a
    failed migration looks like a successful one."""
    res = nodetool("cluster", "upload", "ks1", "table1",
                   expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1"},
                         response=TASK_ID),
        _wait_task(state="failed", error="source node left the cluster")],
                   check_return_code=False)
    assert res.returncode != 0
    assert "source node left the cluster" in res.stdout


def test_cluster_upload_abort(nodetool):
    """--abort must reach the abort endpoint, not the upload one, and must not forward the
    options that only apply to starting a load."""
    nodetool("cluster", "upload", "ks1", "table1", "--abort", "--tablets", "16",
             expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload/abort",
                         params={"ks": "ks1", "table": "table1"})])


def test_cluster_upload_all_options(nodetool):
    nodetool("cluster", "upload", "ks1", "table1", "--tablets", "16", "--primary-replica-only",
             expected_requests=_keyspace_lookups() + [
        expected_request("POST", "/storage_service/tablets/upload",
                         params={"ks": "ks1", "table": "table1",
                                 "tablet_count": "16", "primary_replica_only": "true"},
                         response=TASK_ID),
        _wait_task()])


def test_cluster_upload_rejects_vnode_keyspace(nodetool):
    """A vnode keyspace must be refused before any upload request is sent, with a pointer to
    the command that does work for it."""
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "upload", "ks1", "table1"),
        {"expected_requests": _keyspace_lookups(tablets=False)},
        ["loads only into tablet keyspaces"])


def test_cluster_upload_rejects_multiple_tables(nodetool):
    """The request carries a single table, so more than one has to be rejected rather than
    silently uploading only the first.

    Given as extra positional arguments they are refused by argument parsing before any
    request goes out; the repeated named option is the path that reaches the command's own
    check, so that is what this exercises.
    """
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "upload", "ks1", "--table", "table1", "--table", "table2"),
        {"expected_requests": _keyspace_lookups()},
        ["exactly one table"])


def test_cluster_upload_extra_positional_table_rejected(nodetool):
    """And the positional form is refused too, without contacting the server."""
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "upload", "ks1", "table1", "table2"),
        {"expected_requests": []},
        ["too many positional options"])


def test_cluster_upload_unknown_keyspace(nodetool):
    check_nodetool_fails_with_error_contains(
        nodetool,
        ("cluster", "upload", "no_such_ks", "table1"),
        {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1"])]},
        ["does not exist"])
