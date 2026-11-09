from perf_cap_bench import summarize


def test_latency_uses_successful_reads_and_availability_counts_all_attempts():
    samples = [{"elapsed_ns": n * 1_000_000, "error": ""} for n in range(1, 21)]
    samples.append({"elapsed_ns": 200_000_000, "error": "UNAVAILABLE"})
    row = summarize(samples)
    assert row["median_ms"] == 10.5 and row["p95_ms"] == 19
    assert row["attempted"] == 21 and row["successful"] == 20
    assert row["availability_pct"] == 100 * 20 / 21
    assert row["errors"] == {"UNAVAILABLE": 1}


def test_all_failed_reads_have_no_latency_percentiles():
    row = summarize([{"elapsed_ns": 10, "error": "DEADLINE_EXCEEDED"}])
    assert row["availability_pct"] == 0 and row["median_ms"] is None and row["p95_ms"] is None
