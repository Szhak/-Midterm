import argparse
import csv
import hashlib
import json
import math
import os
import platform
import queue
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, redirect_stderr
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from client import CounterClient, QuorumClient


CONFIGURATIONS = [
    ("Single replica, 1 client", 1, 1),
    ("Single replica, 16 clients", 1, 16),
    ("Quorum (3 replicas), 1 client", 3, 1),
    ("Quorum (3 replicas), 16 clients", 3, 16),
]


def summarize(samples):
    if not samples:
        raise ValueError("No latency samples")
    ordered = sorted(samples)
    index = min(math.ceil(0.95 * len(ordered)), len(ordered) - 1)
    return dict(
        median_ms=statistics.median(ordered), p95_ms=ordered[index], requests=len(ordered)
    )


def split_requests(requests, clients):
    if clients < 1 or requests < clients:
        raise ValueError("Each client needs at least one request")
    return [requests // clients + (index < requests % clients) for index in range(clients)]


@contextmanager
def replicas(count):
    processes = []
    readers = []
    addresses = []
    try:
        for index in range(count):
            process = subprocess.Popen(
                [sys.executable, "-B", str(ROOT / "server.py"), "--port", "0",
                 "--name", f"replica-{chr(65 + index)}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            processes.append(process)
            ready = queue.Queue()
            reader = threading.Thread(
                target=lambda p=process, q=ready: q.put(p.stdout.readline()), daemon=True
            )
            readers.append(reader)
            reader.start()
            banner = ready.get(timeout=15).strip()
            if not banner.startswith("Server listening on 127.0.0.1:"):
                raise RuntimeError(f"Replica did not start: {banner!r}")
            addresses.append(banner.split("Server listening on ", 1)[1])
        yield addresses
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for reader in readers:
            reader.join(timeout=5)
        for process in processes:
            process.stdout.close()


def measure(addresses, clients, requests, warmup):
    counts = split_requests(requests, clients)
    barrier = threading.Barrier(clients)
    stopped = threading.Event()

    def worker(index):
        client = None
        try:
            name = f"bench-{index + 1}"
            if len(addresses) == 1:
                client = CounterClient(addresses[0], name=name)
                channels = [client.channel]
            else:
                client = QuorumClient(addresses, name=name)
                channels = [replica.channel for replica in client.clients.values()]
            for channel in channels:
                grpc.channel_ready_future(channel).result(timeout=10)
            for _ in range(warmup):
                reply = client.incr("warmup", 1)
                if len(addresses) == 3 and reply.acknowledged != 3:
                    raise RuntimeError("A replica failed during warmup")
            barrier.wait(timeout=30)
            samples = []
            start = time.perf_counter_ns()
            end = start
            for operation in range(counts[index]):
                if stopped.is_set():
                    raise RuntimeError("Another benchmark worker failed")
                before = time.perf_counter_ns()
                reply = client.incr("benchmark", 1)
                end = time.perf_counter_ns()
                if len(addresses) == 3 and reply.acknowledged != 3:
                    raise RuntimeError("Degraded quorum during measurement")
                samples.append((index + 1, operation + 1, end - before))
            return samples, start, end
        except BaseException:
            stopped.set()
            barrier.abort()
            raise
        finally:
            if client is not None:
                client.close()

    with ThreadPoolExecutor(max_workers=clients) as pool:
        jobs = [pool.submit(worker, index) for index in range(clients)]
        results = [job.result() for job in jobs]
    samples = [sample for rows, _, _ in results for sample in rows]
    elapsed = (max(end for _, _, end in results) - min(start for _, start, _ in results)) / 1e9
    if len(samples) != requests:
        raise RuntimeError("Wrong sample count")
    values = []
    for address in addresses:
        client = CounterClient(address, name="benchmark-check")
        try:
            state = client.get("benchmark")
            if not state.found or state.value != requests:
                raise RuntimeError(f"Incorrect final state on {address}: {state.value}")
            values.append(state.value)
        finally:
            client.close()
    return samples, elapsed, values


def environment():
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=10
    )
    sources = ("server.py", "client.py", "clocks.py", "counter.proto", "tests/perf_benchmark.py")
    return dict(
        captured_utc=datetime.now(timezone.utc).isoformat(),
        os=platform.platform(), python=platform.python_version(),
        machine=platform.machine(), processor=platform.processor(), logical_cpus=os.cpu_count(),
        packages={name: version(name) for name in ("grpcio", "grpcio-tools", "protobuf", "pytest")},
        base_commit=revision.stdout.strip() if revision.returncode == 0 else None,
        source_sha256={name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources},
    )


def save_results(output, rows, runs, settings):
    with (output / "latencies.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Configuration", "Run", "Client", "Operation", "Latency (ms)"])
        for label, run, client, operation, nanoseconds in rows:
            writer.writerow([label, run, client, operation, f"{nanoseconds / 1e6:.6f}"])
    summary = []
    for label, _, _ in CONFIGURATIONS:
        values = [row[4] / 1e6 for row in rows if row[0] == label]
        matching = [run for run in runs if run["configuration"] == label]
        row = dict(configuration=label, **summarize(values))
        row["throughput_ops_s"] = len(values) / sum(run["elapsed_s"] for run in matching)
        row["median_stddev_ms"] = statistics.pstdev(run["median_ms"] for run in matching)
        row["p95_stddev_ms"] = statistics.pstdev(run["p95_ms"] for run in matching)
        summary.append(row)
    data = dict(environment=environment(), settings=settings, runs=runs, summary=summary)
    (output / "results.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    table = [
        "| Configuration | Median latency (ms) | p95 latency (ms) | Requests |",
        "|---|---:|---:|---:|",
    ]
    for row in summary:
        table.append(
            f"| {row['configuration']} | {row['median_ms']:.3f} | {row['p95_ms']:.3f} | {row['requests']} |"
        )
    median_ratio = summary[2]["median_ms"] / summary[0]["median_ms"]
    loaded_ratio = summary[3]["median_ms"] / summary[1]["median_ms"]
    interpretation = (
        f"The quorum median was {median_ratio:.2f} times the single-replica median with one client "
        f"and {loaded_ratio:.2f} times with 16 clients. "
        "The extra work includes three RPCs per operation, thread dispatch and additional logical-clock logging. "
        "This implementation waits for all three bounded calls to finish before returning, so it measures "
        "that implementation rather than a client returning immediately on the second acknowledgement. "
        "At 16 clients the results also include contention for the shared machine and the servers' eight-worker pools. "
        f"For a counter whose latency budget exceeds the observed worst p95 of {max(r['p95_ms'] for r in summary):.3f} ms, "
        "these local results may be acceptable, but they do not establish performance across a real network or during failures."
    )
    report = [
        "# Performance measurements", "",
        f"{settings['repeats']} fresh runs per configuration, {settings['requests_per_run']} measured logical operations per run.",
        "The table pools the samples across runs; per-run results and population standard deviations are in results.json.",
        f"Each client performs {settings['warmup_per_client']} untimed increments on a separate warmup counter after its channels become ready.",
        "Clients run in threads with one outstanding logical operation per client, starting together at a barrier.",
        "Servers are independent processes on the same machine, using fresh state and ephemeral loopback ports for every run.",
        "Latency uses perf_counter_ns around the complete incr call, including UUID generation, retries and Lamport processing.",
        "Lamport logging stays enabled, with client and server stderr redirected to the null device in every configuration.",
        "Process startup, connection setup, warmup and final-state checks are outside the timed workload.",
        "Every measured quorum operation must receive all three acknowledgements; any exhausted RPC error or wrong final count aborts the run.",
        "p95 follows the assignment literally: sorted samples at zero-based index ceil(0.95 * n); for n=2000 this is sample 1901.",
        "The index is capped at n-1 only for short diagnostic inputs. Median uses the usual average of the middle pair for even n.",
        "Throughput is the number of completed operations divided by the span from the earliest worker start to the latest operation completion.",
        "Raw latencies are in latencies.csv; all replicas' checked final values and environment details are in results.json.",
        "", *table, "", interpretation, "",
    ]
    (output / "performance.md").write_text("\n".join(report), encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "logs" / "performance")
    args = parser.parse_args()
    if args.requests < 2000 or args.repeats < 1 or args.warmup < 0:
        parser.error("Use at least 2000 requests, at least one repeat and nonnegative warmup")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    settings = dict(
        requests_per_run=args.requests, repeats=args.repeats, warmup_per_client=args.warmup,
        latency_clock="perf_counter_ns", transport="localhost gRPC, insecure",
        p95_rule="sorted[min(ceil(0.95*n), n-1)] (zero-based)",
        logging="enabled, stderr redirected to null device", quorum_completion="wait for all three calls",
    )
    rows = []
    runs = []
    for repeat in range(1, args.repeats + 1):
        for label, count, clients in CONFIGURATIONS:
            print(f"Run {repeat}/{args.repeats}: {label}", flush=True)
            with replicas(count) as addresses, open(os.devnull, "w") as sink:
                with redirect_stderr(sink):
                    samples, elapsed, values = measure(addresses, clients, args.requests, args.warmup)
            row = dict(
                configuration=label, run=repeat, **summarize([sample[2] / 1e6 for sample in samples]),
                elapsed_s=elapsed, throughput_ops_s=args.requests / elapsed, final_values=values,
            )
            runs.append(row)
            rows.extend((label, repeat, client, operation, latency) for client, operation, latency in samples)
            print(f"  median={row['median_ms']:.3f} ms p95={row['p95_ms']:.3f} ms values={values}", flush=True)
    summary = save_results(output, rows, runs, settings)
    print(f"Saved {sum(row['requests'] for row in summary)} samples to {output}", flush=True)


if __name__ == "__main__":
    main()
