"""Compare routed leader reads and local follower reads on a healthy loopback cluster."""

import argparse
import csv
import json
import math
import platform
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import grpc

import kvstore_pb2 as kv
from lab_cluster import free_ports, replica_processes


MODES = (kv.LINEARIZABLE, kv.FOLLOWER_STALE)
STALE_MS = 5000
KEY, VALUE = "bench", "confirmed"


def read_sample(client, mode, iteration):
    started = time.perf_counter_ns()
    try:
        reply = client.get(KEY, consistency=mode, max_staleness_ms=STALE_MS if mode == kv.FOLLOWER_STALE else 0)
        elapsed = time.perf_counter_ns() - started
        valid = reply.found and reply.value == VALUE and reply.is_stale == (mode == kv.FOLLOWER_STALE)
        error = "" if valid else "UNEXPECTED_REPLY"
    except grpc.RpcError as failure:
        elapsed = time.perf_counter_ns() - started
        error = failure.code().name
    return {"iteration": iteration, "mode": kv.ReadConsistency.Name(mode),
            "elapsed_ns": elapsed, "error": error}


def summarize(samples):
    timings = sorted(s["elapsed_ns"] / 1_000_000 for s in samples if not s["error"])
    return {
        "attempted": len(samples), "successful": len(timings),
        "availability_pct": 100 * len(timings) / len(samples),
        "median_ms": statistics.median(timings) if timings else None,
        "p95_ms": timings[math.ceil(0.95 * len(timings)) - 1] if timings else None,
        "errors": dict(Counter(s["error"] for s in samples if s["error"])),
    }


def wait_until_seeded(followers):
    for follower in followers:
        end = time.monotonic() + 10
        while time.monotonic() < end:
            if not read_sample(follower, kv.FOLLOWER_STALE, 0)["error"]:
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("Follower never confirmed the seeded value")


def run(iterations, warmup):
    with replica_processes(free_ports()) as (a, b, c):
        result = a.put(KEY, VALUE, strategy=kv.QUORUM_CP)
        if not result.success:
            raise RuntimeError(result.error_message)
        wait_until_seeded((b, c))
        for _ in range(warmup):
            for mode in MODES:
                failure = read_sample(b, mode, 0)["error"]
                if failure:
                    raise RuntimeError(f"Warmup failed: {failure}")
        samples = []
        for iteration in range(1, iterations + 1):
            # Alternate the first mode of each pair to spread time-dependent effects evenly.
            order = MODES if iteration % 2 else reversed(MODES)
            samples.extend(read_sample(b, mode, iteration) for mode in order)
        return samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=1000, help="Measured reads per mode")
    parser.add_argument("--warmup", type=int, default=30, help="Unmeasured reads per mode")
    parser.add_argument("--output-dir", type=Path, default=Path("logs/cap-local"))
    args = parser.parse_args()
    if args.iterations < 1 or args.warmup < 0:
        parser.error("iterations must be positive and warmup must be nonnegative")
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    samples = run(args.iterations, args.warmup)
    results = {kv.ReadConsistency.Name(mode): summarize(
        [s for s in samples if s["mode"] == kv.ReadConsistency.Name(mode)]
    ) for mode in MODES}
    summary = {
        "started_utc": started, "python": platform.python_version(), "grpc": grpc.__version__,
        "system": platform.system(), "replicas": 3, "entry_node": "B", "leaseholder": "A",
        "iterations_per_mode": args.iterations, "warmup_per_mode": args.warmup,
        "max_staleness_ms": STALE_MS, "request_timeout_s": 2.0,
        "percentile": "nearest rank on successful reads", "results": results,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with (args.output_dir / "read_samples.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=["iteration", "mode", "elapsed_ns", "error"])
        writer.writeheader()
        writer.writerows(samples)
    (args.output_dir / "benchmark.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return 0 if all(row["successful"] for row in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
