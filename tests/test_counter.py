import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from client import CounterClient
from server import CounterServicer, start_server


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


@pytest.fixture
def running_clients():
    server, port = start_server(0)
    clients = [CounterClient(f"127.0.0.1:{port}") for _ in range(2)]
    try:
        for client in clients:
            grpc.channel_ready_future(client.channel).result(timeout=5)
        yield clients
    finally:
        for client in clients:
            client.close()
        server.stop(0).wait()


@pytest.fixture
def delayed_client(monkeypatch):
    received_keys = []
    cancelled = threading.Event()
    release = threading.Event()
    original = CounterServicer.Increment

    def delayed_increment(self, request, context):
        result = original(self, request, context)
        with self.lock:
            received_keys.append(request.idempotency_key)
        if not result.was_duplicate:
            context.add_callback(cancelled.set)
            release.wait(timeout=10)
        return result

    monkeypatch.setattr(CounterServicer, "Increment", delayed_increment)
    server, port = start_server(0)
    client = CounterClient(f"127.0.0.1:{port}", timeout=0.5)
    try:
        grpc.channel_ready_future(client.channel).result(timeout=5)
        yield client, received_keys, cancelled
    finally:
        release.set()
        client.close()
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


def test_concurrent_increments_exact(running_clients):
    barrier = threading.Barrier(2)

    def increment_many(client):
        barrier.wait(timeout=5)
        return [client.incr("x", 1).new_value for _ in range(1000)]

    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(increment_many, client) for client in running_clients]
        values = [value for job in jobs for value in job.result(timeout=30)]
    state = running_clients[0].get("x")

    assert sorted(values) == list(range(1, 2001))
    assert state.found is True
    assert state.value == 2000


def test_retry_after_timeout_is_safe(delayed_client, monkeypatch):
    client, received_keys, cancelled = delayed_client
    errors = []
    original = client.stub.Increment

    def record_error(request, **kwargs):
        try:
            return original(request, **kwargs)
        except grpc.RpcError as error:
            errors.append(error.code())
            raise

    monkeypatch.setattr(client.stub, "Increment", record_error)

    result = client.incr("x", 5)
    state = client.get("x")

    assert errors == [grpc.StatusCode.DEADLINE_EXCEEDED]
    assert cancelled.wait(timeout=2)
    assert len(received_keys) == 2
    assert received_keys[0] and received_keys[0] == received_keys[1]
    assert result.new_value == 5
    assert result.was_duplicate is True
    assert state.found is True
    assert state.value == 5
