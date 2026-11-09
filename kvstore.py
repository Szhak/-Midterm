import threading
import time
from dataclasses import dataclass

import grpc

import kvstore_pb2 as kv
import kvstore_pb2_grpc as rpc


@dataclass(frozen=True)
class Record:
    value: str
    timestamp: int
    operation_id: str


class KeyValueStore(rpc.KeyValueStoreServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.values = {}
        self.seen = {}

    def cached_reply(self, request, context):
        previous = self.seen.get(request.idempotency_key)
        if previous is None:
            return None
        original, reply = previous
        if original != request:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Idempotency key reused for another write")
        return reply

    def Put(self, request, context):
        if not request.key or not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Key and idempotency key are required")
        if request.strategy != kv.ALWAYS_AP:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Quorum writes require a replica cluster")
        with self.lock:
            cached = self.cached_reply(request, context)
            if cached is not None:
                return cached
            stamp = request.timestamp or time.time_ns() // 1_000_000
            record = Record(request.value, stamp, request.idempotency_key)
            current = self.values.get(request.key)
            if current is None or (stamp, record.operation_id) > (current.timestamp, current.operation_id):
                self.values[request.key] = record
            reply = kv.PutReply(success=True, acks_received=1)
            self.seen[request.idempotency_key] = (request, reply)
            return reply

    def Get(self, request, context):
        if not request.key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Key is required")
        with self.lock:
            record = self.values.get(request.key)
            if record is None:
                return kv.GetReply(found=False)
            return kv.GetReply(found=True, value=record.value, read_timestamp=record.timestamp)
