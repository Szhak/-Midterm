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


class KeyValueStore(rpc.KeyValueStoreServicer, rpc.ReplicaSyncServicer):
    def __init__(self, port, cluster_ports):
        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.values = {}
        self.seen = {}
        self.pending = None
        self.revision = 0
        self.committed = {}
        self.confirmed_at_ns = None
        self.is_leader = not cluster_ports or port == cluster_ports[0]
        self.channels = {
            peer: grpc.insecure_channel(f"127.0.0.1:{peer}")
            for peer in cluster_ports if peer != port
        }
        self.peers = {peer: rpc.ReplicaSyncStub(channel) for peer, channel in self.channels.items()}
        self.leader = None if self.is_leader else rpc.KeyValueStoreStub(self.channels[cluster_ports[0]])
        self.leader_sync = None if self.is_leader else self.peers[cluster_ports[0]]

    def maintain_replica(self, server):
        try:
            if self.is_leader:
                server.wait_for_termination()
                return
            while True:
                self.refresh_from_leader()
                if not server.wait_for_termination(timeout=0.2):
                    return
        finally:
            for channel in self.channels.values():
                channel.close()

    def refresh_from_leader(self):
        with self.lock:
            known = self.revision
        # Starting the freshness window before the round trip includes any network delay.
        started_ns = time.monotonic_ns()
        try:
            snapshot = self.leader_sync.Fetch(kv.ReplicaAck(revision=known), timeout=0.4)
        except grpc.RpcError:
            return
        with self.lock:
            if snapshot.revision >= self.revision:
                self.install_snapshot(snapshot)
                self.confirmed_at_ns = started_ns

    @staticmethod
    def forward(method, request, context):
        try:
            return method(request, timeout=min(context.time_remaining(), 2.0))
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

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
        if request.strategy == kv.QUORUM_CP:
            if self.is_leader:
                return self.quorum_put(request, context)
            return self.forward(self.leader.Put, request, context)
        if request.strategy != kv.ALWAYS_AP:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unknown write strategy")
        with self.write_lock, self.lock:
            cached = self.cached_reply(request, context)
            if cached is not None:
                return cached
            stamp = request.timestamp or time.time_ns() // 1_000_000
            record = Record(request.value, stamp, request.idempotency_key)
            current = self.values.get(request.key)
            if current is None or (stamp, record.operation_id) > (current.timestamp, current.operation_id):
                self.values[request.key] = record
                if self.is_leader:
                    self.committed[request.key] = record
                    self.revision += 1
            reply = kv.PutReply(success=True, acks_received=1)
            self.seen[request.idempotency_key] = (request, reply)
            return reply

    def quorum_put(self, request, context):
        # One fixed leader orders CP writes. Staging a candidate never changes a readable value.
        with self.write_lock:
            with self.lock:
                cached = self.cached_reply(request, context)
                if cached is not None:
                    return cached
            staged = []
            attempts = {p: stub.Stage.future(request, timeout=0.5) for p, stub in self.peers.items()}
            for port, attempt in attempts.items():
                try:
                    attempt.result()
                    staged.append(port)
                except grpc.RpcError:
                    continue
            acks = 1 + len(staged)
            if acks < 2:
                return kv.PutReply(acks_received=acks, error_message="Quorum loss: need 2 of 3 replicas")
            with self.lock:
                stamp = request.timestamp or time.time_ns() // 1_000_000
                record = Record(request.value, stamp, request.idempotency_key)
                self.committed[request.key] = self.values[request.key] = record
                self.revision += 1
                reply = kv.PutReply(success=True, acks_received=acks)
                self.seen[request.idempotency_key] = (request, reply)
                snapshot = self.snapshot()
            notifications = [self.peers[p].Publish.future(snapshot, timeout=0.5) for p in staged]
            for notification in notifications:
                try:
                    notification.result()
                except grpc.RpcError:
                    # The decision is already committed. A missed publication can be fetched later.
                    pass
            return reply

    def snapshot(self):
        return kv.CommittedSnapshot(
            revision=self.revision,
            values=[kv.CommittedValue(key=k, value=v.value, timestamp=v.timestamp, operation_id=v.operation_id)
                    for k, v in self.committed.items()],
        )

    def Stage(self, request, context):
        if self.is_leader:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Only followers stage proposals")
        if request.strategy != kv.QUORUM_CP or not request.key or not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "A valid CP proposal is required")
        with self.lock:
            if (self.pending is not None and self.pending.idempotency_key == request.idempotency_key
                    and self.pending != request):
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Conflicting staged operation")
            self.pending = request
            return kv.ReplicaAck(revision=self.revision)

    def install_snapshot(self, snapshot):
        if snapshot.revision <= self.revision:
            return
        self.committed = {
            entry.key: Record(entry.value, entry.timestamp, entry.operation_id)
            for entry in snapshot.values
        }
        self.revision = snapshot.revision
        if self.pending is not None:
            committed = self.committed.get(self.pending.key)
            if committed and committed.operation_id == self.pending.idempotency_key:
                self.pending = None

    def Publish(self, request, context):
        if self.is_leader:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Only followers install snapshots")
        with self.lock:
            self.install_snapshot(request)
            return kv.ReplicaAck(revision=self.revision)

    def Fetch(self, request, context):
        if not self.is_leader:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Fetch committed state from the leaseholder")
        with self.lock:
            if request.revision == self.revision:
                return kv.CommittedSnapshot(revision=self.revision)
            return self.snapshot()

    def Get(self, request, context):
        if not request.key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Key is required")
        if request.consistency not in (kv.LINEARIZABLE, kv.FOLLOWER_STALE):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Unknown read consistency")
        if not self.is_leader and request.consistency == kv.LINEARIZABLE:
            return self.forward(self.leader.Get, request, context)

        with self.lock:
            if not self.is_leader:
                limit_ns = request.max_staleness_ms * 1_000_000
                if self.confirmed_at_ns is None or time.monotonic_ns() - self.confirmed_at_ns > limit_ns:
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "STALE_READ_EXCEEDED")
            record = self.committed.get(request.key)
            if record is None:
                return kv.GetReply(found=False, is_stale=not self.is_leader)
            return kv.GetReply(found=True, value=record.value, read_timestamp=record.timestamp,
                               is_stale=not self.is_leader)
