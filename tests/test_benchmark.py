from types import SimpleNamespace

import pytest

import perf_benchmark
from perf_benchmark import split_requests, summarize


def test_benchmark_percentiles():
    samples = list(reversed(range(2000)))

    result = summarize(samples)

    assert result == dict(median_ms=999.5, p95_ms=1900, requests=2000)
    assert samples[0] == 1999


@pytest.mark.parametrize("requests, clients", [(2000, 1), (2000, 16), (2001, 16)])
def test_benchmark_request_distribution(requests, clients):
    counts = split_requests(requests, clients)

    assert len(counts) == clients
    assert sum(counts) == requests
    assert min(counts) > 0
    assert max(counts) - min(counts) <= 1


def test_benchmark_empty_samples_rejected():
    with pytest.raises(ValueError, match="No latency samples"):
        summarize([])


def test_benchmark_keeps_successful_retry_samples(monkeypatch):
    class RetriedClient:
        def __init__(self, address, name):
            self.channel = object()

        def incr(self, counter_id, delta):
            return SimpleNamespace(was_duplicate=True)

        def get(self, counter_id):
            return SimpleNamespace(found=True, value=20)

        def close(self):
            pass

    monkeypatch.setattr(perf_benchmark, "CounterClient", RetriedClient)
    monkeypatch.setattr(
        perf_benchmark.grpc, "channel_ready_future",
        lambda channel: SimpleNamespace(result=lambda timeout: None),
    )

    samples, elapsed, values = perf_benchmark.measure(["test"], 1, 20, 0)

    assert len(samples) == 20
    assert [sample[1] for sample in samples] == list(range(1, 21))
    assert elapsed > 0
    assert values == [20]
