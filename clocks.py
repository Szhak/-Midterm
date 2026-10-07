import sys
import threading


class LamportClock:
    def __init__(self, name):
        self.name = name
        self.value = 0
        self.lock = threading.Lock()

    def event(self, kind, detail, received=None):
        with self.lock:
            if received is not None:
                self.value = max(self.value, received)
            self.value += 1
            suffix = "" if received is None else f"  (received L={received})"
            print(
                f"[{self.name}] {kind:<6} {detail}  L={self.value}{suffix}",
                file=sys.stderr,
                flush=True,
            )
            return self.value
