import argparse
import uuid

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc


class CounterClient:
    def __init__(self, address="127.0.0.1:50051"):
        self.channel = grpc.insecure_channel(address)
        self.stub = rpc.CounterStub(self.channel)

    def incr(self, counter_id, delta, key=None):
        request = pb.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key if key is not None else str(uuid.uuid4()),
        )
        return self.stub.Increment(request)

    def get(self, counter_id):
        return self.stub.Get(pb.GetRequest(counter_id=counter_id))

    def close(self):
        self.channel.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    commands = parser.add_subparsers(dest="command", required=True)
    incr = commands.add_parser("incr")
    incr.add_argument("counter_id")
    incr.add_argument("--by", type=int, default=1)
    incr.add_argument("--key")
    get = commands.add_parser("get")
    get.add_argument("counter_id")
    args = parser.parse_args()
    client = CounterClient(f"127.0.0.1:{args.port}")
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
    finally:
        client.close()


if __name__ == "__main__":
    main()
