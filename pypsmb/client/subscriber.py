import threading
import socket
import struct
import error
from typing import Union, Iterator, Callable
from ..util import read_exactly, read_cstring


class Subscriber(threading.Thread):

    _protocol_version = 1

    def __init__(self, host: str, port: int, topic: str,
                 handler: Union[Iterator[Callable[[bytes], None]], Callable[[bytes], None]]):
        super().__init__()
        self._host = host
        self._port = port
        self._topic = topic
        self._close_event = threading.Event()
        if isinstance(handler, Callable):
            self._handlers = [handler]
        else:
            self._handlers = list([x for x in handler])

    def _connect(self):
        s = socket.socket()
        s.connect((self._host, self._port))

        s.sendall(b'PSMB')
        s.sendall(struct.pack('I', socket.htonl(self._protocol_version)))
        s.sendall(b'\x00\x00\x00\x00')

        response = read_cstring(s)
        if response == b'UNSUPPORTED PROTOCOL':
            raise RuntimeError('')


        self._sock = s

    def start(self) -> None:
        self._connect()
        super().start()

    def run(self) -> None:
        while not self._close_event.is_set():
            pass

    def close(self):
        self._close_event.set()
        self.join()





