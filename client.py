import argparse
import sys
import time
import uuid

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


class CounterClient:
    def __init__(self, address="127.0.0.1:50051", timeout=2.0, name="client-1"):
        self.channel = grpc.insecure_channel(address)
        self.stub = rpc.CounterStub(self.channel)
        self.timeout = timeout
        self.clock = LamportClock(name)

    def incr(self, counter_id, delta, key=None):
        request = pb.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key if key is not None else str(uuid.uuid4()),
        )
        return self._call(self.stub.Increment, request)

    def get(self, counter_id):
        return self._call(self.stub.Get, pb.GetRequest(counter_id=counter_id))

    def _call(self, method, request):
        if isinstance(request, pb.IncrementRequest):
            detail = f"Increment(counter={request.counter_id}, delta={request.delta})"
        else:
            detail = f"Get(counter={request.counter_id})"
        for attempt in range(4):
            request.lamport_time = self.clock.event("SEND", detail)
            try:
                reply = method(request, timeout=self.timeout)
                if isinstance(reply, pb.IncrementReply):
                    detail_reply = f"IncrementReply(new_value={reply.new_value})"
                else:
                    detail_reply = f"GetReply(value={reply.value}, found={reply.found})"
                self.clock.event("RECV", detail_reply, received=reply.lamport_time)
                return reply
            except grpc.RpcError as error:
                if error.code() not in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                ) or attempt == 3:
                    raise
                time.sleep(0.2 * (2 ** attempt))

    def close(self):
        self.channel.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="client-1")
    commands = parser.add_subparsers(dest="command", required=True)
    incr = commands.add_parser("incr")
    incr.add_argument("counter_id")
    incr.add_argument("--by", type=int, default=1)
    incr.add_argument("--key")
    get = commands.add_parser("get")
    get.add_argument("counter_id")
    args = parser.parse_args()
    client = CounterClient(f"127.0.0.1:{args.port}", name=args.name)
    try:
        if args.command == "incr":
            result = client.incr(args.counter_id, args.by, args.key)
            duplicate = "yes" if result.was_duplicate else "no"
            print(f"OK value={result.new_value} duplicate={duplicate}")
        else:
            result = client.get(args.counter_id)
            if result.found:
                print(f"value={result.value}")
            else:
                print("Counter not found")
    except grpc.RpcError as error:
        print(f"RPC failed: {error.code().name}: {error.details()}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
