import socket
from asyncio import IncompleteReadError


def read_exactly(sock: socket.socket, num_bytes: int) -> bytes:
    buf = bytearray(num_bytes)
    pos = 0
    while pos < num_bytes:
        n = sock.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), num_bytes)
        pos += n
    return bytes(buf)


def read_cstring(sock: socket.socket, max_bytes: int = -1) -> bytes:
    buffer = b''
    size = 0
    while (c := sock.recv(1)) != b'\0':
        buffer += c
        size += 1
        if size >= max_bytes > 0:
            break
    return buffer
