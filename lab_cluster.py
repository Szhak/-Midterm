"""Launch an isolated three-process lab cluster on loopback ports."""

import queue
import socket
import subprocess
import sys
import threading
from contextlib import ExitStack, contextmanager
from pathlib import Path

import grpc

from client import KeyValueClient


def free_ports():
    with ExitStack() as cleanup:
        sockets = [cleanup.enter_context(socket.socket()) for _ in range(3)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return tuple(sock.getsockname()[1] for sock in sockets)


def stop_process(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    process.stdout.close()


@contextmanager
def replica_processes(ports, routes=None):
    server = Path(__file__).with_name("server.py")
    with ExitStack() as cleanup:
        clients = []
        for i, port in enumerate(ports):
            peers = ports if routes is None else routes[i]
            process = subprocess.Popen(
                [sys.executable, "-u", str(server), "--port", str(port), "--name", "ABC"[i],
                 "--cluster-ports", *map(str, peers)],
                stdout=subprocess.PIPE, text=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            cleanup.callback(stop_process, process)
            banner = queue.Queue()
            threading.Thread(target=lambda p=process, q=banner: q.put(p.stdout.readline()),
                             daemon=True).start()
            if banner.get(timeout=15).strip() != f"Server listening on 127.0.0.1:{port}":
                raise RuntimeError(f"Replica {'ABC'[i]} did not start on port {port}")
            client = KeyValueClient(f"127.0.0.1:{port}")
            cleanup.callback(client.close)
            clients.append(client)
        for client in clients:
            grpc.channel_ready_future(client.channel).result(timeout=10)
        yield clients
