import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


class CounterServicer(rpc.CounterServicer):
    def __init__(self, name="replica-A"):
        self.values = {}
        self.seen = {}
        self.lock = threading.Lock()
        self.clock = LamportClock(name)

    def Increment(self, request, context):
        with self.lock:
            self.clock.event(
                "RECV",
                f"Increment(counter={request.counter_id}, delta={request.delta})",
                received=request.lamport_time,
            )
            if request.idempotency_key in self.seen:
                value = self.seen[request.idempotency_key]
                duplicate = True
            else:
                value = self.values.get(request.counter_id, 0) + request.delta
                self.clock.event("APPLY", f"counter={request.counter_id} -> {value}")
                self.values[request.counter_id] = value
                self.seen[request.idempotency_key] = value
                duplicate = False
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


def start_server(port, name="replica-A"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(CounterServicer(name), server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    if bound_port == 0:
        raise RuntimeError(f"Cannot bind port {port}")
    server.start()
    return server, bound_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    args = parser.parse_args()
    server, port = start_server(args.port, args.name)
    print(f"Server listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
