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
from client import CounterClient, QuorumClient, QuorumError
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


@pytest.fixture
def replica_group():
    servers = []
    addresses = []
    client = None
    try:
        for name in ("replica-A", "replica-B", "replica-C"):
            server, port = start_server(0, name=name)
            servers.append(server)
            addresses.append(f"127.0.0.1:{port}")
        client = QuorumClient(addresses, timeout=0.5, name="quorum-1")
        for replica in client.clients.values():
            grpc.channel_ready_future(replica.channel).result(timeout=5)
        yield servers, client
    finally:
        if client is not None:
            client.close()
        for server in servers:
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


@pytest.mark.parametrize("stopped", [0, 1, 2], ids=["A-down", "B-down", "C-down"])
def test_majority_commit_two_acks(replica_group, stopped):
    servers, client = replica_group
    addresses = list(client.clients)
    servers[stopped].stop(0).wait()
    live = set(addresses) - {addresses[stopped]}

    result = client.incr("x", 5, key="two-acks")
    states = [client.clients[address].get("x") for address in live]

    assert result.acknowledged == 2
    assert set(result.replies) == live
    assert result.key == "two-acks"
    assert result.new_value == 5
    assert result.was_duplicate is False
    assert all(reply.new_value == 5 for reply in result.replies.values())
    assert all(state.found and state.value == 5 for state in states)


@pytest.mark.parametrize(
    "survivor", [0, 1, 2, None], ids=["only-A", "only-B", "only-C", "all-down"]
)
def test_no_commit_below_majority(replica_group, survivor):
    servers, client = replica_group
    addresses = list(client.clients)
    stopped = {address for i, address in enumerate(addresses) if i != survivor}
    for index, server in enumerate(servers):
        if index != survivor:
            server.stop(0).wait()

    with pytest.raises(QuorumError) as first:
        client.incr("x", 5)
    before_retry = (
        client.clients[addresses[survivor]].get("x") if survivor is not None else None
    )
    with pytest.raises(QuorumError) as second:
        client.incr("x", 5, key=first.value.key)
    after_retry = (
        client.clients[addresses[survivor]].get("x") if survivor is not None else None
    )

    assert first.value.key
    assert second.value.key == first.value.key
    for error in (first.value, second.value):
        assert error.acknowledged == (0 if survivor is None else 1)
        assert set(error.errors) == stopped
        assert not hasattr(error, "new_value")
        assert str(error).startswith("NOT committed:")
    if survivor is not None:
        assert before_retry.found and before_retry.value == 5
        assert after_retry.found and after_retry.value == 5


def test_replicas_converge(replica_group):
    _, first = replica_group
    second = QuorumClient(first.clients, timeout=0.5, name="quorum-2")
    barrier = threading.Barrier(2)

    def write_batch(client, writer):
        barrier.wait(timeout=5)
        results = []
        for index in range(50):
            counter, delta = ("x", 1) if index % 2 == 0 else ("y", 2)
            results.append(client.incr(counter, delta, key=f"batch-{writer}-{index}"))
        return results

    try:
        for replica in second.clients.values():
            grpc.channel_ready_future(replica.channel).result(timeout=5)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(write_batch, client, i) for i, client in enumerate((first, second))]
            results = [result for job in jobs for result in job.result(timeout=30)]
        duplicates = [first.incr("x", 1, key=f"batch-{writer}-0") for writer in (0, 1)]
        states = [
            (replica.get("x"), replica.get("y")) for replica in first.clients.values()
        ]
    finally:
        second.close()

    assert len(results) == 100
    assert all(result.acknowledged == 3 for result in results)
    assert all(set(result.replies) == set(first.clients) for result in results)
    assert all(result.acknowledged == 3 and result.was_duplicate for result in duplicates)
    assert all(x.found and y.found for x, y in states)
    assert [(x.value, y.value) for x, y in states] == [(50, 100)] * 3


def test_counter_ids_are_isolated(running_server):
    x = pb.IncrementRequest(counter_id="x", delta=7, idempotency_key="x-key")
    y = pb.IncrementRequest(counter_id="y", delta=-2, idempotency_key="y-key")

    running_server.Increment(x, timeout=2)
    running_server.Increment(y, timeout=2)
    x_state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)
    y_state = running_server.Get(pb.GetRequest(counter_id="y"), timeout=2)
    missing = running_server.Get(pb.GetRequest(counter_id="z"), timeout=2)

    assert x_state.found and x_state.value == 7
    assert y_state.found and y_state.value == -2
    assert missing.found is False


def test_concurrent_duplicate_requests(running_server):
    barrier = threading.Barrier(16)

    def repeat_request():
        request = pb.IncrementRequest(counter_id="x", delta=7, idempotency_key="same-key")
        barrier.wait(timeout=10)
        return running_server.Increment(request, timeout=5)

    with ThreadPoolExecutor(max_workers=16) as pool:
        jobs = [pool.submit(repeat_request) for _ in range(16)]
        replies = [job.result(timeout=10) for job in jobs]
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert all(reply.new_value == 7 for reply in replies)
    assert sum(not reply.was_duplicate for reply in replies) == 1
    assert sum(reply.was_duplicate for reply in replies) == 15
    assert state.found and state.value == 7


@pytest.mark.parametrize("delta", [0, -5], ids=["zero", "negative"])
def test_zero_and_negative_deltas(running_server, delta):
    request = pb.IncrementRequest(counter_id="x", delta=delta, idempotency_key="delta-key")

    result = running_server.Increment(request, timeout=2)
    duplicate = running_server.Increment(request, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert result.new_value == delta and result.was_duplicate is False
    assert duplicate.new_value == delta and duplicate.was_duplicate is True
    assert state.found and state.value == delta


def test_duplicate_returns_original_result(running_server):
    first = pb.IncrementRequest(counter_id="x", delta=5, idempotency_key="first-key")
    later = pb.IncrementRequest(counter_id="x", delta=3, idempotency_key="later-key")

    original = running_server.Increment(first, timeout=2)
    updated = running_server.Increment(later, timeout=2)
    duplicate = running_server.Increment(first, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert original.new_value == 5
    assert updated.new_value == 8
    assert duplicate.was_duplicate is True and duplicate.new_value == 5
    assert state.value == 8


@pytest.mark.parametrize(
    "boundary, delta", [(2 ** 63 - 1, 1), (-(2 ** 63), -1)], ids=["upper", "lower"]
)
def test_overflow_does_not_change_state(running_server, boundary, delta):
    seed = pb.IncrementRequest(counter_id="x", delta=boundary, idempotency_key="seed")
    request = pb.IncrementRequest(counter_id="x", delta=delta, idempotency_key="overflow")
    move_away = pb.IncrementRequest(counter_id="x", delta=-delta, idempotency_key="move-away")
    running_server.Increment(seed, timeout=2)

    with pytest.raises(grpc.RpcError) as error:
        running_server.Increment(request, timeout=2)

    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE
    unchanged = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)
    assert unchanged.found and unchanged.value == boundary

    moved = running_server.Increment(move_away, timeout=2)
    retry = running_server.Increment(request, timeout=2)
    duplicate = running_server.Increment(request, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert moved.new_value == boundary - delta
    assert retry.new_value == boundary and retry.was_duplicate is False
    assert duplicate.new_value == boundary and duplicate.was_duplicate is True
    assert state.value == boundary


def test_empty_idempotency_key_rejected(running_server):
    invalid = pb.IncrementRequest(counter_id="x", delta=3)
    valid = pb.IncrementRequest(counter_id="x", delta=3, idempotency_key="valid-key")

    with pytest.raises(grpc.RpcError) as error:
        running_server.Increment(invalid, timeout=2)
    missing = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)
    result = running_server.Increment(valid, timeout=2)
    state = running_server.Get(pb.GetRequest(counter_id="x"), timeout=2)

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert missing.found is False
    assert result.new_value == 3 and result.was_duplicate is False
    assert state.found and state.value == 3
