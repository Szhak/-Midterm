import argparse
import json
import multiprocessing as mp
import re
import sys
from contextlib import redirect_stderr
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from client import CounterClient
from server import start_server


def serve(output, ready, stop):
    with (output / "replica-A.log").open("w", encoding="utf-8") as log:
        with redirect_stderr(log):
            server, port = start_server(0, name="replica-A")
            try:
                ready.put(port)
                stop.wait(timeout=60)
            finally:
                server.stop(0).wait()


def run_client(number, address, output, barrier, results):
    name = f"client-{number}"
    with (output / f"{name}.log").open("w", encoding="utf-8") as log:
        with redirect_stderr(log):
            client = CounterClient(address, name=name)
            try:
                grpc.channel_ready_future(client.channel).result(timeout=10)
                barrier.wait(timeout=10)
                if number in (1, 2):
                    counter = "x" if number == 1 else "y"
                    values = [client.incr(counter, 1).new_value for _ in range(2)]
                    assert values == [1, 2]
                else:
                    values = [client.get(counter).value for counter in ("x", "y")]
                    assert all(0 <= value <= 2 for value in values)
                results.put((name, values))
            finally:
                client.close()


def find_path(edges, start, end):
    pending = [(start, [start])]
    visited = set()
    while pending:
        node, path = pending.pop()
        if node == end:
            return path
        if node not in visited:
            visited.add(node)
            pending.extend((b, path + [b]) for a, b in edges if a == node)
    return []


def analyze(output, results):
    pattern = re.compile(
        r"\[(.+)\] (SEND|RECV|APPLY)\s+(.+?)  L=(\d+)"
        r"(?:  \(received L=(\d+)\))?"
    )
    events = []
    local_edges = []
    for name in ("client-1", "client-2", "client-3", "replica-A"):
        previous = None
        clock = 0
        lines = (output / f"{name}.log").read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines, 1):
            match = pattern.fullmatch(line)
            assert match is not None, line
            node, kind, detail, stamp, received = match.groups()
            assert node == name
            stamp = int(stamp)
            received = None if received is None else int(received)
            assert (kind == "RECV") == (received is not None)
            assert stamp == max(clock, received or 0) + 1
            clock = stamp
            event_id = f"{name}:{index}"
            events.append(dict(
                id=event_id, node=node, kind=kind, detail=detail,
                time=stamp, received=received, line=line,
            ))
            if previous:
                local_edges.append([previous, event_id])
            previous = event_id

    assert len(events) == 28
    sends = [event for event in events if event["kind"] == "SEND"]
    message_edges = []
    for event in events:
        if event["kind"] != "RECV":
            continue
        matches = [send for send in sends if (
            send["node"] != event["node"]
            and send["time"] == event["received"]
            and send["detail"] == event["detail"]
        )]
        assert len(matches) == 1, event
        message_edges.append([matches[0]["id"], event["id"]])
    assert len(message_edges) == 12
    assert sorted(event["detail"] for event in events if event["kind"] == "APPLY") == [
        "counter=x -> 1", "counter=x -> 2", "counter=y -> 1", "counter=y -> 2",
    ]

    edges = local_edges + message_edges
    causal_chains = []
    for name in ("client-1", "client-2"):
        second_receive = next(b for a, b in message_edges if a == f"{name}:3")
        chain = find_path(edges, f"{name}:2", second_receive)
        assert chain
        causal_chains.append(chain)
    clients = [event for event in events if event["node"].startswith("client-")]
    concurrent = next(
        [a["id"], b["id"]] for a in clients for b in clients
        if a["node"] != b["node"] and a["time"] != b["time"]
        and not find_path(edges, a["id"], b["id"])
        and not find_path(edges, b["id"], a["id"])
    )
    merged = sorted(events, key=lambda event: (event["time"], event["node"]))
    (output / "merged.log").write_text(
        "\n".join(event["line"] for event in merged) + "\n", encoding="utf-8"
    )
    analysis = dict(
        results=results, events=events, local_edges=local_edges,
        message_edges=message_edges, causal_chains=causal_chains,
        concurrent_pair=concurrent,
        merge_note="Sorted by Lamport time and node for display, not by wall-clock time. "
        "Only local and message edges establish happens-before.",
        concurrency_note="Neither event is reachable from the other through the recorded "
        "local and message edges, although their Lamport values differ.",
        limitation="Lamport values alone cannot distinguish causality from concurrency. "
        "Vector clocks can distinguish these relations.",
    )
    (output / "analysis.json").write_text(
        json.dumps(analysis, indent=2) + "\n", encoding="utf-8"
    )
    return analysis


def run_scenario(output):
    output.mkdir(parents=True, exist_ok=True)
    context = mp.get_context("spawn")
    ready = context.Queue()
    results = context.Queue()
    stop = context.Event()
    barrier = context.Barrier(3)
    server = context.Process(target=serve, args=(output, ready, stop))
    workers = []
    server.start()
    try:
        port = ready.get(timeout=15)
        for number in (1, 2, 3):
            worker = context.Process(
                target=run_client,
                args=(number, f"127.0.0.1:{port}", output, barrier, results),
            )
            worker.start()
            workers.append(worker)
        for worker in workers:
            worker.join(timeout=15)
            if worker.exitcode != 0:
                raise RuntimeError(f"Client failed or timed out: {worker.exitcode}")
        observed = dict(results.get(timeout=5) for _ in workers)
    finally:
        stop.set()
        for worker in workers + [server]:
            worker.join(timeout=5)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        ready.close()
        results.close()
    if server.exitcode != 0:
        raise RuntimeError(f"Server failed: {server.exitcode}")
    return analyze(output, observed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir", type=Path, default=Path(__file__).resolve().parents[1] / "logs"
    )
    args = parser.parse_args()
    analysis = run_scenario(args.output_dir.resolve())
    print(f"OK: {len(analysis['events'])} events, x=2, y=2")
    print(f"Logs: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
