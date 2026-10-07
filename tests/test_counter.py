import grpc
import pytest

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from server import start_server


@pytest.fixture
def running_server():
    server, port = start_server(0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        yield rpc.CounterStub(channel)
    finally:
        channel.close()
        server.stop(0).wait()


def test_increment_applies_delta(running_server):
    request = pb.IncrementRequest(counter_id="x", delta=5, idempotency_key="k-1")

    result = running_server.Increment(request, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert result.new_value == 5
    assert result.was_duplicate is False
    assert state.found is True
    assert state.value == 5


def test_get_missing_counter(running_server):
    request = pb.GetRequest(counter_id="missing")

    result = running_server.Get(request, timeout=2)

    assert result.found is False
    assert result.value == 0


def test_duplicate_key_not_reapplied(running_server):
    request = pb.IncrementRequest(counter_id="x", delta=5, idempotency_key="k-1")

    first = running_server.Increment(request, timeout=2)
    second = running_server.Increment(request, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert first.new_value == 5
    assert second.new_value == 5
    assert second.was_duplicate is True
    assert state.value == 5
