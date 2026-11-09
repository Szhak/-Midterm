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
