import select
import socket
import socketserver
import threading


class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        link = self.server
        try:
            with socket.create_connection(("127.0.0.1", link.target), timeout=1) as upstream:
                pair = (self.request, upstream)
                with link.lock:
                    if link.blocked:
                        return
                    link.active.add(pair)
                try:
                    while True:
                        ready, _, _ = select.select(pair, [], [])
                        for source in ready:
                            data = source.recv(65536)
                            if not data:
                                return
                            dest = upstream if source is self.request else self.request
                            dest.sendall(data)
                finally:
                    with link.lock:
                        link.active.discard(pair)
        except OSError:
            # Closing either end is expected when a link is cut or a process stops.
            return


class TcpLink(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, target):
        super().__init__(("127.0.0.1", 0), Relay)
        self.target = target
        self.port = self.server_address[1]
        self.lock = threading.Lock()
        self.active = set()
        self.blocked = False
        self.thread = threading.Thread(target=self.serve_forever, args=(0.05,), daemon=True)
        self.thread.start()

    def set_blocked(self, blocked):
        with self.lock:
            self.blocked = blocked
            if blocked:
                for pair in self.active:
                    for sock in pair:
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass

    def __exit__(self, *args):
        self.set_blocked(True)
        self.shutdown()
        self.thread.join()
        super().__exit__(*args)
