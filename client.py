import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import grpc

import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


class CounterClient:
    def __init__(self, address="127.0.0.1:50051", timeout=2.0, name="client-1", clock=None):
        self.channel = grpc.insecure_channel(address)
        self.stub = rpc.CounterStub(self.channel)
        self.timeout = timeout
        self.clock = clock if clock is not None else LamportClock(name)

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


@dataclass
class QuorumResult:
    replies: dict
    key: str

    @property
    def acknowledged(self):
        return len(self.replies)

    @property
    def new_value(self):
        values = {reply.new_value for reply in self.replies.values()}
        return values.pop() if len(values) == 1 else None

    @property
    def was_duplicate(self):
        return all(reply.was_duplicate for reply in self.replies.values())


class QuorumError(Exception):
    def __init__(self, acknowledged, key, errors):
        self.acknowledged = acknowledged
        self.key = key
        self.errors = errors
        super().__init__(f"NOT committed: acks={acknowledged}/3, key={key}")


class QuorumClient:
    def __init__(self, addresses, timeout=2.0, name="client-1"):
        addresses = tuple(addresses)
        if len(addresses) != 3 or len(set(addresses)) != 3:
            raise ValueError("Three distinct replica addresses are required")
        self.clock = LamportClock(name)
        self.clients = {
            address: CounterClient(address, timeout, name, self.clock)
            for address in addresses
        }
        self.pool = ThreadPoolExecutor(max_workers=3)

    def incr(self, counter_id, delta, key=None):
        key = key if key is not None else str(uuid.uuid4())
        jobs = {
            address: self.pool.submit(client.incr, counter_id, delta, key)
            for address, client in self.clients.items()
        }
        replies = {}
        errors = {}
        for address, job in jobs.items():
            try:
                replies[address] = job.result()
            except grpc.RpcError as error:
                errors[address] = error.code().name
        if len(replies) < 2:
            raise QuorumError(len(replies), key, errors)
        return QuorumResult(replies, key)

    def get(self, counter_id):
        return next(iter(self.clients.values())).get(counter_id)

    def close(self):
        self.pool.shutdown(wait=True)
        for client in self.clients.values():
            client.close()


def main():
    parser = argparse.ArgumentParser()
    connection = parser.add_mutually_exclusive_group()
    connection.add_argument("--port", type=int, default=50051)
    connection.add_argument("--ports", type=int, nargs=3)
    parser.add_argument("--name", default="client-1")
    commands = parser.add_subparsers(dest="command", required=True)
    incr = commands.add_parser("incr")
    incr.add_argument("counter_id")
    incr.add_argument("--by", type=int, default=1)
    incr.add_argument("--key")
    get = commands.add_parser("get")
    get.add_argument("counter_id")
    args = parser.parse_args()
    ports = args.ports if args.ports is not None else [args.port]
    if any(port < 1 or port > 65535 for port in ports):
        parser.error("Ports must be between 1 and 65535")
    if len(set(ports)) != len(ports):
        parser.error("Replica ports must be distinct")
    if args.ports is not None:
        client = QuorumClient([f"127.0.0.1:{port}" for port in ports], name=args.name)
    else:
        client = CounterClient(f"127.0.0.1:{args.port}", name=args.name)
    try:
        if args.command == "incr":
            result = client.incr(args.counter_id, args.by, args.key)
            duplicate = "yes" if result.was_duplicate else "no"
            if isinstance(result, QuorumResult):
                if result.new_value is not None:
                    value = f"value={result.new_value}"
                else:
                    values = ", ".join(
                        f"{address}={reply.new_value}" for address, reply in result.replies.items()
                    )
                    value = f"replica_values=[{values}]"
                print(
                    f"OK committed {value} (replicas acked: {result.acknowledged}/3, "
                    f"duplicate: {duplicate}) key={result.key}"
                )
            else:
                print(f"OK value={result.new_value} duplicate={duplicate}")
        else:
            result = client.get(args.counter_id)
            if result.found:
                print(f"value={result.value}")
            else:
                print("Counter not found")
    except QuorumError as error:
        print(str(error), file=sys.stderr)
        return 1
    except grpc.RpcError as error:
        print(f"RPC failed: {error.code().name}: {error.details()}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
