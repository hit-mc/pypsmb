import logging
import re
import socket
import struct
import select
from asyncio import IncompleteReadError

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


def _publish(sock: socket.socket, addr, dispatcher: MessageDispatcher,
             topic_id: str, keep_alive: float, logger):
    while True:
        logger.info('Waiting for client command...')
        command = read_exactly(sock, 3)
        if command == b'NOP':
            logger.info('Client NOP.')
            sock.sendall(b'NIL')
            logger.info('NIL is sent.')
        elif command == b'NIL':
            logger.info('Client is OK.')
            pass
        elif command == b'BYE':
            logger.info('Bye bye.')
            break
        elif command == b'MSG':
            logger.info('Receiving message...')
            message_length = int.from_bytes(read_exactly(sock, 8), NETWORK_BYTEORDER, signed=False)
            logger.info('Length: %d', message_length)
            message = read_exactly(sock, message_length)
            assert isinstance(message, bytes)
            logger.info('Message: %s', message)
            dispatcher.publish(message, topic_id)


def _subscribe(sock: socket.socket, addr, dispatcher: MessageDispatcher, subscriber_id: int, pattern: str,
               keep_alive: float, logger):
    if keep_alive > 0:
        # sock.settimeout(keep_alive)
        logger.info('Wait timeout: %fs.', keep_alive)
    rsock = dispatcher.subscribe(subscriber_id, pattern)
    try:
        while True:
            rlist, _, _ = select.select([sock, rsock], [], [], keep_alive if (keep_alive > 0) else None)
            if not rlist:
                # timeout
                # send keep-alive
                logger.info('Send NOP.')
                sock.sendall(b'NOP')
            for ready_sock in rlist:
                if ready_sock is sock:
                    # the client is ready
                    command = read_exactly(sock, 3)
                    if command == b'NIL':
                        logger.info('Client is OK.')
                    elif command == b'NOP':
                        logger.info('Client NOP.')
                        sock.sendall(b'NIL')
                        logger.info('NIL is sent.')
                    else:
                        logger.error('Invalid command from client: %s', command)
                        return
                else:
                    # messages are ready
                    rsock.recv(1)  # read out the mark, this should complete immediately
                    for message, topic in dispatcher.read_inbox(subscriber_id):
                        logger.info('Message size: %d, topic: %s', len(message), topic)
                        sock.sendall(b'MSG')
                        sock.sendall(len(message).to_bytes(8, NETWORK_BYTEORDER, signed=False))
                        sock.sendall(message)
    finally:
        rsock.close()
        dispatcher.unsubscribe(subscriber_id)
        logger.info('Removed subscriber %d.', subscriber_id)


def handle_client(sock: socket.socket, addr, dispatcher: MessageDispatcher, keep_alive: float):
    logger = logging.getLogger('%s:%d' % addr)
    try:
        with sock:
            logger.info('Handling client...')

            if read_exactly(sock, 4) != b'PSMB':
                logger.info('Bad protocol magic')
                return

            protocol = socket.ntohl(struct.unpack('I', read_exactly(sock, 4))[0])
            logger.info('Protocol version: %d', protocol)
            if protocol != 1:
                logger.info('Unsupported protocol')
                sock.sendall(b'UNSUPPORTED PROTOCOL\0')
                return

            options = read_exactly(sock, 4)
            if options != b'\x00\x00\x00\x00':
                logger.info('Bad options')
                return

            sock.sendall(b'OK\0\x00\x00\x00\x00')
            logger.info('Handshake complete.')

            while True:
                mode = read_exactly(sock, 3)
                if mode == b'PUB':
                    topic_id = read_cstring(sock)
                    try:
                        topic_id = topic_id.decode('ascii')
                    except UnicodeDecodeError:
                        sock.sendall(b'FAILED\0' + b'Cannot decode topic id string with ASCII.\0')
                        continue
                    sock.sendall(b'OK\0')
                    logger.info('Switched to PUBLISH mode.')
                    _publish(sock, addr, dispatcher, topic_id, keep_alive, logger)
                    break
                elif mode == b'SUB':
                    options = socket.ntohl(struct.unpack('I', read_exactly(sock, 4))[0])
                    id_pattern = read_cstring(sock)
                    subscriber_id = int.from_bytes(read_exactly(sock, 8), NETWORK_BYTEORDER, signed=False) \
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
                    logger.info('Switched to SUBSCRIBE mode.')
                    _subscribe(sock, addr, dispatcher, subscriber_id, id_pattern, keep_alive, logger)
                    break
                else:
                    sock.sendall(b'BAD COMMAND\0')
                    break

    except IncompleteReadError:
        pass
    except Exception as e:
        logger.error('Uncaught exception: %s', repr(e))
    logger.info('Handler exited.')