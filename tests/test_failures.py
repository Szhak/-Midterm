import queue
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest

from client import CounterClient, QuorumClient


@pytest.fixture
def replica_process(tmp_path):
    root = Path(__file__).resolve().parents[1]
    processes = []
    files = []
    readers = []

    def launch(fault="none", delay_ms=3000):
        name = f"replica-{chr(65 + len(processes))}"
        log_path = tmp_path / f"{name}.log"
        log = log_path.open("w", encoding="utf-8")
        files.append(log)
        process = subprocess.Popen(
            [sys.executable, "-B", str(root / "server.py"), "--port", "0",
             "--name", name, "--fault", fault, "--delay-ms", str(delay_ms)],
            cwd=root, stdout=subprocess.PIPE, stderr=log, text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        processes.append(process)
        output = queue.Queue()
        reader = threading.Thread(target=lambda: output.put(process.stdout.readline()), daemon=True)
        readers.append(reader)
        reader.start()
        banner = output.get(timeout=10).strip()
        assert banner.startswith("Server listening on 127.0.0.1:"), log_path.read_text(encoding="utf-8")
        address = banner.split("Server listening on ", 1)[1]
        return SimpleNamespace(process=process, address=address, log=log_path)

    try:
        yield launch
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for reader in readers:
            reader.join(timeout=5)
        for process in processes:
            process.stdout.close()
        for log in files:
            log.close()


def save_evidence(folder, name, result, replicas):
    lines = [name, result]
    for replica in replicas:
        lines.extend(replica.log.read_text(encoding="utf-8").splitlines())
    (folder / "evidence.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_replica_crash_mid_request(replica_process, tmp_path):
    victim = replica_process("delay-after-recv", delay_ms=20000)
    live = [replica_process(), replica_process()]
    client = QuorumClient([victim.address, *(replica.address for replica in live)], timeout=10)
    try:
        for replica in client.clients.values():
            grpc.channel_ready_future(replica.channel).result(timeout=5)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(client.incr, "x", 5, "crash-key")
            limit = time.monotonic() + 5
            while time.monotonic() < limit:
                trace = victim.log.read_text(encoding="utf-8")
                if "FAULT  delay-after-recv" in trace:
                    break
                time.sleep(0.01)
            assert "RECV   Increment(counter=x, delta=5)" in trace
            assert "FAULT  delay-after-recv" in trace
            assert "APPLY" not in trace
            assert victim.process.poll() is None
            assert not pending.done()
            victim.process.kill()
            victim.process.wait(timeout=5)
            result = pending.result(timeout=12)
        states = [client.clients[replica.address].get("x") for replica in live]
    finally:
        client.close()

    assert victim.process.returncode != 0
    assert result.acknowledged == 2
    assert set(result.replies) == {replica.address for replica in live}
    assert result.new_value == 5
    assert all(state.found and state.value == 5 for state in states)
    assert "APPLY" not in victim.log.read_text(encoding="utf-8")
    save_evidence(
        tmp_path, "test_replica_crash_mid_request",
        f"killed_after_recv=True acks={result.acknowledged}/3 live_values={[s.value for s in states]}",
        [victim, *live],
    )


def test_request_duplication(replica_process, tmp_path):
    replica = replica_process()
    client = CounterClient(replica.address)
    try:
        grpc.channel_ready_future(client.channel).result(timeout=5)

        first = client.incr("x", 5, key="duplicate-key")
        second = client.incr("x", 5, key="duplicate-key")
        state = client.get("x")
    finally:
        client.close()

    assert first.new_value == 5 and first.was_duplicate is False
    assert second.new_value == 5 and second.was_duplicate is True
    assert state.found and state.value == 5
    assert replica.log.read_text(encoding="utf-8").count("APPLY") == 1
    save_evidence(
        tmp_path, "test_request_duplication",
        f"second_duplicate={second.was_duplicate} final_value={state.value} apply_count=1",
        [replica],
    )


def test_induced_timeout_with_retry(replica_process, monkeypatch, tmp_path):
    replica = replica_process("delay-after-apply", delay_ms=3000)
    client = CounterClient(replica.address, timeout=0.5)
    errors = []
    keys = []
    original = client.stub.Increment

    def record_attempt(request, **kwargs):
        keys.append(request.idempotency_key)
        try:
            return original(request, **kwargs)
        except grpc.RpcError as error:
            errors.append(error.code())
            raise

    monkeypatch.setattr(client.stub, "Increment", record_attempt)
    try:
        grpc.channel_ready_future(client.channel).result(timeout=5)

        result = client.incr("x", 5, key="timeout-key")
        state = client.get("x")
    finally:
        client.close()

    assert errors == [grpc.StatusCode.DEADLINE_EXCEEDED]
    assert keys == ["timeout-key", "timeout-key"]
    assert result.was_duplicate is True and result.new_value == 5
    assert state.found and state.value == 5
    trace = replica.log.read_text(encoding="utf-8")
    assert trace.count("FAULT") == 1
    assert trace.count("APPLY") == 1
    assert trace.count("RECV   Increment") == 2
    save_evidence(
        tmp_path, "test_induced_timeout_with_retry",
        f"errors={[error.name for error in errors]} keys={keys} duplicate={result.was_duplicate} final_value={state.value}",
        [replica],
    )
