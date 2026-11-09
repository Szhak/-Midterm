import socket
import time
from contextlib import ExitStack
from itertools import combinations

import grpc
import pytest

import kvstore_pb2 as kv
import kvstore_pb2_grpc as kv_rpc
from client import KeyValueClient
from kvstore import KeyValueStore
from lab_cluster import free_ports, replica_processes
from server import start_server
from tcp_link import TcpLink


@pytest.fixture
def local_client():
    server, port = start_server(0)
    client = KeyValueClient(f"127.0.0.1:{port}")
    try:
        yield client
    finally:
        client.close()
        server.stop(0).wait()


def test_local_value_and_missing_key(local_client):
    assert not local_client.get("x").found
    reply = local_client.put("x", "", strategy=kv.ALWAYS_AP, timestamp=100)
    assert reply.success and reply.acks_received == 1
    result = local_client.get("x")
    assert result.found and result.value == "" and result.read_timestamp == 100


def test_duplicate_put_preserves_newer_value(local_client):
    write = dict(strategy=kv.ALWAYS_AP, operation_id="first", timestamp=100)
    first = local_client.put("x", "old", **write)
    local_client.put("x", "new", strategy=kv.ALWAYS_AP, timestamp=101)
    assert local_client.put("x", "old", **write) == first
    assert local_client.get("x").value == "new"
    with pytest.raises(grpc.RpcError) as failure:
        local_client.put("x", "different", **write)
    assert failure.value.code() == grpc.StatusCode.ALREADY_EXISTS


@pytest.fixture
def cluster():
    # Reserve distinct ephemeral loopback ports before assigning roles A, B, C.
    with ExitStack() as reservations:
        sockets = [reservations.enter_context(socket.socket()) for _ in range(3)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        ports = tuple(sock.getsockname()[1] for sock in sockets)
    with ExitStack() as cleanup:
        servers, clients = [], []
        for port in ports:
            server, _ = start_server(port, cluster_ports=ports)
            cleanup.callback(lambda s=server: s.stop(0).wait())
            servers.append(server)
            client = KeyValueClient(f"127.0.0.1:{port}")
            cleanup.callback(client.close)
            clients.append(client)
        for client in clients:
            grpc.channel_ready_future(client.channel).result(timeout=5)
        yield servers, clients


def test_cp_majority_and_retry_through_follower(cluster):
    servers, (a, b, _) = cluster
    servers[2].stop(0).wait()
    args = dict(strategy=kv.QUORUM_CP, operation_id="first")
    reply = b.put("x", "one", **args)
    assert reply.success and reply.acks_received == 2
    assert a.get("x").value == "one"
    assert a.put("x", "two", strategy=kv.QUORUM_CP).success
    assert b.put("x", "one", **args) == reply
    assert a.get("x").value == "two"


def test_cp_quorum_loss_keeps_last_committed_value(cluster):
    servers, (a, _, _) = cluster
    assert a.put("x", "confirmed", strategy=kv.QUORUM_CP).success
    for server in servers[1:]:
        server.stop(0).wait()
    result = a.put("x", "unconfirmed", strategy=kv.QUORUM_CP)
    assert not result.success and result.acks_received == 1
    assert "Quorum loss" in result.error_message
    assert a.get("x").value == "confirmed"


def test_follower_cannot_confirm_cp_without_leader(cluster):
    servers, (_, b, _) = cluster
    servers[0].stop(0).wait()
    with pytest.raises(grpc.RpcError) as failure:
        b.put("x", "unconfirmed", strategy=kv.QUORUM_CP)
    assert failure.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


def wait_for_follower(client, key, value):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            result = client.get(key, consistency=kv.FOLLOWER_STALE, max_staleness_ms=5000)
            if result.found and result.value == value:
                assert result.is_stale
                return result
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
                raise
        time.sleep(0.025)
    pytest.fail(f"Follower did not confirm {key}={value!r}")


def test_linearizable_read_sees_write_while_follower_lags(cluster, monkeypatch):
    _, (a, b, _) = cluster
    assert a.put("x", "first", strategy=kv.QUORUM_CP, timestamp=1).success
    assert wait_for_follower(b, "x", "first").read_timestamp == 1

    # Freeze installation on the followers while RPC traffic to the leader still works.
    with monkeypatch.context() as delay:
        delay.setattr(KeyValueStore, "install_snapshot", lambda self, snapshot: None)
        delay.setattr(KeyValueStore, "refresh_from_leader", lambda self: None)
        assert a.put("x", "second", strategy=kv.QUORUM_CP).success
        strong = b.get("x", consistency=kv.LINEARIZABLE)
        assert strong.value == "second" and not strong.is_stale
        local = b.get("x", consistency=kv.FOLLOWER_STALE, max_staleness_ms=5000)
        assert local.value == "first" and local.is_stale
    wait_for_follower(b, "x", "second")


def test_follower_reads_only_within_verified_window(cluster):
    servers, (a, b, _) = cluster
    assert a.put("x", "confirmed", strategy=kv.QUORUM_CP).success
    wait_for_follower(b, "x", "confirmed")
    servers[0].stop(0).wait()
    local = b.get("x", consistency=kv.FOLLOWER_STALE, max_staleness_ms=5000)
    assert local.found and local.is_stale
    with pytest.raises(grpc.RpcError) as strong:
        b.get("x", consistency=kv.LINEARIZABLE)
    assert strong.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
    # The only authority for a new snapshot is now stopped.
    time.sleep(0.06)
    for key in ("x", "missing"):
        with pytest.raises(grpc.RpcError) as expired:
            b.get(key, consistency=kv.FOLLOWER_STALE, max_staleness_ms=50)
        assert expired.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert expired.value.details() == "STALE_READ_EXCEEDED"


def test_acknowledged_proposal_is_hidden_until_committed(cluster):
    _, (a, b, _) = cluster
    assert a.put("x", "old", strategy=kv.QUORUM_CP).success
    wait_for_follower(b, "x", "old")
    replica = kv_rpc.ReplicaSyncStub(b.channel)
    replica.Stage(kv.PutRequest(key="x", value="tentative", idempotency_key="pending"), timeout=1)
    assert b.get("x", consistency=kv.FOLLOWER_STALE, max_staleness_ms=5000).value == "old"
    assert b.get("x", consistency=kv.LINEARIZABLE).value == "old"


@pytest.fixture
def partition_cluster():
    # Clients address the servers directly. Only links crossing {A,B} | {C}
    # traverse TCP relays, in both directions (including forwarded client RPCs).
    a, b, c = ports = free_ports()
    with ExitStack() as cleanup:
        to_a, to_b, to_c = links = [cleanup.enter_context(TcpLink(p)) for p in ports]
        routes = [(a, b, to_c.port), (a, b, to_c.port), (to_a.port, to_b.port, c)]
        clients = cleanup.enter_context(replica_processes(ports, routes))

        def partition(blocked):
            for link in links:
                link.set_blocked(blocked)

        yield clients, partition


def test_live_partition_cp_and_quorum_intersection(partition_cluster):
    (a, b, c), partition = partition_cluster
    assert a.put("x", "before", strategy=kv.QUORUM_CP, timestamp=100).success
    wait_for_follower(c, "x", "before")
    partition(True)
    majority = b.put("x", "after", strategy=kv.QUORUM_CP, timestamp=200)
    assert majority.success and majority.acks_received == 2
    with pytest.raises(grpc.RpcError) as refused:
        c.put("x", "minority", strategy=kv.QUORUM_CP, timestamp=300)
    assert refused.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)

    def local(node):
        return node.get("x", consistency=kv.FOLLOWER_STALE, max_staleness_ms=60_000)

    assert [local(node).value for node in (a, b, c)] == ["after", "after", "before"]
    for pair in combinations((a, b, c), 2):
        newest = max((local(node) for node in pair), key=lambda reply: reply.read_timestamp)
        assert newest.value == "after"  # R=2, W=2; every pair intersects {A,B}.
    assert local(c).value == "before"  # R=1, W=2; {C} is disjoint.
    partition(False)
    wait_for_follower(c, "x", "after")
