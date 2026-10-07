import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc


class CounterServicer(rpc.CounterServicer):
    def __init__(self):
        self.values = {}
        self.seen = {}
        self.lock = threading.Lock()

    def Increment(self, request, context):
        with self.lock:
            if request.idempotency_key in self.seen:
                value = self.seen[request.idempotency_key]
                return pb.IncrementReply(new_value=value, was_duplicate=True)
            value = self.values.get(request.counter_id, 0) + request.delta
            self.values[request.counter_id] = value
            self.seen[request.idempotency_key] = value
            return pb.IncrementReply(new_value=value, was_duplicate=False)

    def Get(self, request, context):
        with self.lock:
            return pb.GetReply(
                value=self.values.get(request.counter_id, 0),
                found=request.counter_id in self.values,
            )


def start_server(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(CounterServicer(), server)
    bound_port = server.add_insecure_port(f"127.0.0.1:{port}")
    if bound_port == 0:
        raise RuntimeError(f"Cannot bind port {port}")
    server.start()
    return server, bound_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()
    server, port = start_server(args.port)
    print(f"Server listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
