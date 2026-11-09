import socket
from contextlib import ExitStack

import grpc
import pytest

import kvstore_pb2 as kv
from client import KeyValueClient
from server import start_server


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
