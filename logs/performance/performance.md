# Performance measurements

3 fresh runs per configuration, 2000 measured logical operations per run.
The table pools the samples across runs; per-run results and population standard deviations are in results.json.
Each client performs 20 untimed increments on a separate warmup counter after its channels become ready.
Clients run in threads with one outstanding logical operation per client, starting together at a barrier.
Servers are independent processes on the same machine, using fresh state and ephemeral loopback ports for every run.
Latency uses perf_counter_ns around the complete incr call, including UUID generation, retries and Lamport processing.
Lamport logging stays enabled, with client and server stderr redirected to the null device in every configuration.
Process startup, connection setup, warmup and final-state checks are outside the timed workload.
Every measured quorum operation must receive all three acknowledgements; any exhausted RPC error or wrong final count aborts the run.
p95 follows the assignment literally: sorted samples at zero-based index ceil(0.95 * n); for n=2000 this is sample 1901.
The index is capped at n-1 only for short diagnostic inputs. Median uses the usual average of the middle pair for even n.
Throughput is the number of completed operations divided by the span from the earliest worker start to the latest operation completion.
Raw latencies are in latencies.csv; all replicas' checked final values and environment details are in results.json.

| Configuration | Median latency (ms) | p95 latency (ms) | Requests |
|---|---:|---:|---:|
| Single replica, 1 client | 1.645 | 1.989 | 6000 |
| Single replica, 16 clients | 11.246 | 12.425 | 6000 |
| Quorum (3 replicas), 1 client | 2.667 | 2.999 | 6000 |
| Quorum (3 replicas), 16 clients | 37.701 | 61.793 | 6000 |

The quorum median was 1.62 times the single-replica median with one client and 3.35 times with 16 clients. The extra work includes three RPCs per operation, thread dispatch and additional logical-clock logging. This implementation waits for all three bounded calls to finish before returning, so it measures that implementation rather than a client returning immediately on the second acknowledgement. At 16 clients the results also include contention for the shared machine and the servers' eight-worker pools. For a counter whose latency budget exceeds the observed worst p95 of 61.793 ms, these local results may be acceptable, but they do not establish performance across a real network or during failures.
