import threading

class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Incrementa o relógio local (ocorre antes de enviar um evento/mensagem)."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp: int) -> int:
        """Atualiza o relógio ao receber uma mensagem."""
        with self._lock:
            self._value = max(self._value, received_timestamp) + 1
            return self._value

    @property
    def time(self) -> int:
        with self._lock:
            return self._value