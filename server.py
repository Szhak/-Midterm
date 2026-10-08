import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


FAULTS = ("none", "delay-after-recv", "delay-after-apply")


class CounterServicer(rpc.CounterServicer):
    def __init__(self, name="replica-A", fault="none", delay_ms=3000):
        if fault not in FAULTS or delay_ms < 0:
            raise ValueError("Unknown fault or negative delay")
        self.values = {}
        self.seen = {}
        self.lock = threading.Lock()
        self.clock = LamportClock(name)
        self.fault = fault
        self.delay_ms = delay_ms
        self.fault_used = False

    def inject_delay(self, phase, key, context):
        with self.lock:
            if self.fault != phase or self.fault_used:
                return
            self.fault_used = True
            self.clock.event("FAULT", f"{phase} delay_ms={self.delay_ms} key={key}")
        cancelled = threading.Event()
        if not context.add_callback(cancelled.set):
            cancelled.set()
        if cancelled.wait(timeout=self.delay_ms / 1000):
            context.abort(grpc.StatusCode.CANCELLED, "Cancelled during injected delay")

    def Increment(self, request, context):
        with self.lock:
            self.clock.event(
                "RECV",
                f"Increment(counter={request.counter_id}, delta={request.delta})",
                received=request.lamport_time,
            )
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Idempotency key is required")
        self.inject_delay("delay-after-recv", request.idempotency_key, context)
        with self.lock:
            if request.idempotency_key in self.seen:
                value = self.seen[request.idempotency_key]
                duplicate = True
            else:
                value = self.values.get(request.counter_id, 0) + request.delta
                if not -(2 ** 63) <= value <= 2 ** 63 - 1:
                    context.abort(grpc.StatusCode.OUT_OF_RANGE, "Counter exceeds int64 range")
                self.clock.event("APPLY", f"counter={request.counter_id} -> {value}")
                self.values[request.counter_id] = value
                self.seen[request.idempotency_key] = value
                duplicate = False
        if not duplicate:
            self.inject_delay("delay-after-apply", request.idempotency_key, context)
        with self.lock:
            stamp = self.clock.event("SEND", f"IncrementReply(new_value={value})")
            return pb.IncrementReply(
                new_value=value, was_duplicate=duplicate, lamport_time=stamp
            )

    def Get(self, request, context):
        with self.lock:
            self.clock.event(
                "RECV", f"Get(counter={request.counter_id})",
                received=request.lamport_time,
            )
            value = self.values.get(request.counter_id, 0)
            found = request.counter_id in self.values
            stamp = self.clock.event("SEND", f"GetReply(value={value}, found={found})")
            return pb.GetReply(
                value=value, found=found, lamport_time=stamp,
            )


def start_server(port, name="replica-A", fault="none", delay_ms=3000):
    servicer = CounterServicer(name, fault, delay_ms)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    if bound_port == 0:
        raise RuntimeError(f"Cannot bind port {port}")
    server.start()
    return server, bound_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--fault", choices=FAULTS, default="none")
    parser.add_argument("--delay-ms", type=int, default=3000)
    args = parser.parse_args()
    if args.delay_ms < 0:
        parser.error("Delay must be nonnegative")
    server, port = start_server(args.port, args.name, args.fault, args.delay_ms)
    print(f"Server listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
