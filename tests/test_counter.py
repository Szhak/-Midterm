import json
import re
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import pytest

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from client import CounterClient
from clocks import LamportClock
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
    clients = [
        CounterClient(f"127.0.0.1:{port}", name=f"client-{i + 1}") for i in range(2)
    ]
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
    sent_times = []
    original = client.stub.Increment

    def record_error(request, **kwargs):
        sent_times.append(request.lamport_time)
        try:
            return original(request, **kwargs)
        except grpc.RpcError as error:
            errors.append(error.code())
            raise

    monkeypatch.setattr(client.stub, "Increment", record_error)

    result = client.incr("x", 5)
    state = client.get("x")

    assert errors == [grpc.StatusCode.DEADLINE_EXCEEDED]
    assert 0 < sent_times[0] < sent_times[1]
    assert cancelled.wait(timeout=2)
    assert len(received_keys) == 2
    assert received_keys[0] and received_keys[0] == received_keys[1]
    assert result.new_value == 5
    assert result.was_duplicate is True
    assert state.found is True
    assert state.value == 5


def test_lamport_clock_rules(capsys):
    clock = LamportClock("node")

    first = clock.event("SEND", "first")
    ahead = clock.event("RECV", "ahead", received=10)
    behind = clock.event("RECV", "behind", received=2)
    local = clock.event("APPLY", "change")

    assert (first, ahead, behind, local) == (1, 11, 12, 13)
    assert capsys.readouterr().err.splitlines() == [
        "[node] SEND   first  L=1",
        "[node] RECV   ahead  L=11  (received L=10)",
        "[node] RECV   behind  L=12  (received L=2)",
        "[node] APPLY  change  L=13",
    ]


def test_lamport_clock_thread_safe():
    clock = LamportClock("node")
    barrier = threading.Barrier(2)

    def record_events():
        barrier.wait(timeout=5)
        return [clock.event("SEND", "event") for _ in range(100)]

    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(record_events) for _ in range(2)]
        stamps = [stamp for job in jobs for stamp in job.result(timeout=5)]

    assert sorted(stamps) == list(range(1, 201))
    assert clock.value == 200


def test_lamport_rpc_trace(running_clients, capsys):
    first, second = running_clients

    initial = first.incr("x", 5, "clock-key")
    duplicate = first.incr("x", 5, "clock-key")
    state = first.get("x")
    other_state = second.get("x")
    lines = capsys.readouterr().err.splitlines()

    assert initial.lamport_time == 4
    assert duplicate.lamport_time == 8
    assert duplicate.was_duplicate is True
    assert state.lamport_time == 12
    assert other_state.lamport_time == 14
    assert first.clock.value == 13
    assert second.clock.value == 15
    assert lines[:5] == [
        "[client-1] SEND   Increment(counter=x, delta=5)  L=1",
        "[replica-A] RECV   Increment(counter=x, delta=5)  L=2  (received L=1)",
        "[replica-A] APPLY  counter=x -> 5  L=3",
        "[replica-A] SEND   IncrementReply(new_value=5)  L=4",
        "[client-1] RECV   IncrementReply(new_value=5)  L=5  (received L=4)",
    ]
    assert len(lines) == 17
    assert sum("APPLY" in line for line in lines) == 1
    assert "[client-2] SEND   Get(counter=x)  L=1" in lines
    for line in lines:
        received = re.search(r"L=(\d+)  \(received L=(\d+)\)", line)
        if received:
            assert int(received[1]) > int(received[2])


@pytest.mark.parametrize(
    "message", [pb.IncrementRequest, pb.IncrementReply, pb.GetRequest, pb.GetReply]
)
def test_lamport_proto_field(message):
    stamp = 2 ** 40
    value = message(lamport_time=stamp)

    restored = message.FromString(value.SerializeToString())

    field = message.DESCRIPTOR.fields_by_name["lamport_time"]
    assert field.number == 4
    assert field.type == field.TYPE_UINT64
    assert restored.lamport_time == stamp


def test_three_client_clock_scenario(tmp_path):
    root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "-B", str(root / "tests" / "clock_scenario.py"),
         "--output-dir", str(tmp_path)],
        cwd=root, capture_output=True, text=True, timeout=45,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    analysis = json.loads((tmp_path / "analysis.json").read_text(encoding="utf-8"))
    assert analysis["results"]["client-1"] == [1, 2]
    assert analysis["results"]["client-2"] == [1, 2]
    assert len(analysis["results"]["client-3"]) == 2
    assert all(0 <= value <= 2 for value in analysis["results"]["client-3"])
    merged = (tmp_path / "merged.log").read_text(encoding="utf-8").splitlines()
    assert len(merged) == 28
    assert sum("APPLY" in line for line in merged) == 4
    assert len(analysis["causal_chains"]) == 2
    assert len(analysis["message_edges"]) == 12
    a, b = analysis["concurrent_pair"]
    events = {event["id"]: event for event in analysis["events"]}
    assert events[a]["node"] != events[b]["node"]
    assert events[a]["time"] != events[b]["time"]
    reader_log = (tmp_path / "client-3.log").read_text(encoding="utf-8")
    assert "Increment" not in reader_log
    assert reader_log.count("SEND") == 2
