import re
import socket
import struct
from asyncio import IncompleteReadError
from typing import TextIO

from .message_dispatcher import MessageDispatcher

NETWORK_BYTEORDER = 'big'


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


def validate_pattern(s: str):
    try:
        re.compile(s)
    except re.error:
        return False
    return True


def _publish(sock: socket.socket, addr, dispatcher: MessageDispatcher, rf: TextIO, topic_id: str, keep_alive: float):
    while True:
        command = rf.read(3)
        if command == b'NOP':
            sock.sendall(b'NIL')
        elif command == b'NIL':
            pass
        elif command == b'BYE':
            break
        elif command == b'MSG':
            print('Receiving message...')
            message_length = int.from_bytes(rf.read(8), NETWORK_BYTEORDER, signed=False)
            print('Length:', message_length)
            message = rf.read(message_length)
            assert isinstance(message, bytes)
            print('Message: ', message)
            dispatcher.publish(message, topic_id)


def _subscribe(sock: socket.socket, addr, dispatcher: MessageDispatcher, rf: TextIO, subscriber_id: int, pattern: str,
               keep_alive: float):
    event = dispatcher.subscribe(subscriber_id, pattern)
    while True:
        if not event.wait(keep_alive if (keep_alive > 0) else None):
            # timeout
            # send keep-alive
            sock.sendall(b'NOP')
            if rf.read(3) != b'NIL':
                return
        # new message to dispatch
        for message, topic in dispatcher.read_inbox(subscriber_id):
            print('Size:', len(message))
            print('Topic:', topic)
            sock.sendall(b'MSG')
            sock.sendall(len(message).to_bytes(8, NETWORK_BYTEORDER, signed=False))
            sock.sendall(message)


def handle_client(sock: socket.socket, addr, dispatcher: MessageDispatcher, keep_alive: float):
    try:
        with sock, sock.makefile('rb') as rf:
            print('Handling client...')

            if rf.read(4) != b'PSMB':
                print('Bad protocol magic')
                return

            protocol = socket.ntohl(struct.unpack('I', rf.read(4))[0])
            print('Protocol:', protocol)
            if protocol != 1:
                print('Unsupported protocol')
                sock.sendall(b'UNSUPPORTED PROTOCOL\0')
                return

            options = rf.read(4)
            if options != b'\x00\x00\x00\x00':
                print('Bad options')
                return

            sock.sendall(b'OK\0\x00\x00\x00\x00')

            while True:
                mode = rf.read(3)
                if mode == b'PUB':
                    topic_id = read_cstring(sock)
                    try:
                        topic_id = topic_id.decode('ascii')
                    except UnicodeDecodeError:
                        sock.sendall(b'FAILED\0' + b'Cannot decode topic id string with ASCII.\0')
                        continue
                    sock.sendall(b'OK\0')
                    _publish(sock, addr, dispatcher, rf, topic_id, keep_alive)
                    break
                elif mode == b'SUB':
                    options = socket.ntohl(struct.unpack('I', rf.read(4))[0])
                    id_pattern = read_cstring(sock)
                    subscriber_id = int.from_bytes(rf.read(8), NETWORK_BYTEORDER, signed=False) \
                        if (options & 1) else None
                    try:
                        id_pattern = id_pattern.decode('ascii')
                    except UnicodeDecodeError:
                        sock.sendall(b'FAILED\0' + b'Cannot decode pattern string with ASCII.\0')
                        continue
                    if not validate_pattern(id_pattern):
                        sock.sendall(b'FAILED\0' + b'Invalid pattern string.\0')
                        continue
                    sock.sendall(b'OK\0')
                    _subscribe(sock, addr, dispatcher, rf, subscriber_id, id_pattern, keep_alive)
                    break
                else:
                    sock.sendall(b'BAD COMMAND\0')
                    break

    except IncompleteReadError:
        pass
    except Exception as e:
        print('Uncaught exception:', e)
