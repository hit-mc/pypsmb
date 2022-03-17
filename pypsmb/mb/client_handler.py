import logging
import re
import socket
import struct
import select
from asyncio import IncompleteReadError

from .message_dispatcher import MessageDispatcher, SubscriberAlreadyExistsError
from ..util import read_exactly, read_cstring

NETWORK_BYTEORDER = 'big'


class InvalidMessageError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


def validate_pattern(s: str):
    try:
        re.compile(s)
    except re.error:
        return False
    return True


def _publish(sock: socket.socket, addr, dispatcher: MessageDispatcher,
             topic_id: str, keep_alive: float, max_pending_keepalive: int = 3,
             protocol: int = 1):
    logger = logging.getLogger('publish,%s:%d' % addr)
    pending_keepalive_count = 0  # how many continuous NOP did we sent, which is not responded by the client
    while True:
        logger.info('Waiting client for commands...')
        rlist, _, _ = select.select([sock], [], [], keep_alive if (keep_alive > 0) else None)
        if not rlist:
            if protocol != 2:
                # protocol v1 does not support NOP sent from passive peer
                continue
            # timeout, send keepalive
            if pending_keepalive_count == max_pending_keepalive:
                # the client is not sensible
                # kick it
                logger.error('Insensible client (too many pending keepalive responses). Kick it.')
                break
            logger.info('Send NOP. (keepalive)')
            sock.sendall(b'NOP')
            logger.info('NOP is sent.')
            pending_keepalive_count += 1
            continue
        command = read_exactly(sock, 3)
        if command == b'NOP':
            logger.info('Client NOP.')
            sock.sendall(b'NIL')
            logger.info('Responded with NIL.')
        elif command == b'NIL':
            logger.info('Client NIL. Client is OK.')
            pending_keepalive_count = 0
            pass
        elif command == b'BYE':
            logger.info('Client BYE. Disconnecting.')
            break
        elif command == b'MSG':
            logger.info('Receiving new message from client...')
            message_length = int.from_bytes(read_exactly(sock, 8), NETWORK_BYTEORDER, signed=False)
            logger.info('Message length: %d byte(s).', message_length)
            message = read_exactly(sock, message_length)
            logger.info('Topic: %s, Message: %s.', topic_id, message)
            dispatcher.publish(message, topic_id)
        else:
            raise InvalidMessageError(f'Invalid command from client: {command}')


def _subscribe(sock: socket.socket, addr, dispatcher: MessageDispatcher, subscriber_id: int, pattern: str,
               keep_alive: float, max_pending_keepalive: int = 3):
    logger = logging.getLogger('subscribe,%s:%d' % addr)
    if keep_alive > 0:
        # sock.settimeout(keep_alive)
        logger.info('Keepalive is enabled. Set socket timeout to %fs.', keep_alive)
    rsock = dispatcher.subscribe(subscriber_id, pattern)
    pending_keepalive_count = 0  # how many continuous NOP did we sent, which is not responded by the client
    try:
        while True:
            rlist, _, _ = select.select([sock, rsock], [], [], keep_alive if (keep_alive > 0) else None)
            if not rlist:
                # timeout
                # send keep-alive
                if pending_keepalive_count == max_pending_keepalive:
                    # the client is not sensible
                    # kick it
                    logger.error('Insensible client (too many pending keepalive responses). Kick it.')
                    break
                logger.info('Send NOP. (keepalive)')
                sock.sendall(b'NOP')
                logger.info('NOP is sent.')
                pending_keepalive_count += 1
                continue
            for ready_sock in rlist:
                if ready_sock is sock:
                    # the client is ready
                    command = read_exactly(sock, 3)
                    if command == b'NIL':
                        logger.info('Client NIL. Client is OK.')
                        pending_keepalive_count = 0
                    elif command == b'NOP':
                        logger.info('Client NOP.')
                        sock.sendall(b'NIL')
                        logger.info('Responded with NIL.')
                    elif command == b'BYE':
                        logger.info('Client BYE. Disconnecting.')
                        break
                    else:
                        raise InvalidMessageError(f'Invalid command from client: {command}')
                else:
                    # messages are ready
                    rsock.recv(1)  # read out the mark, this should complete immediately
                    for message, topic in dispatcher.read_inbox(subscriber_id):
                        logger.info('Message size: %d, topic: %s', len(message), topic)
                        sock.sendall(b'MSG')
                        sock.sendall(len(message).to_bytes(8, NETWORK_BYTEORDER, signed=False))
                        sock.sendall(message)
    except Exception:
        logger.exception('An exception occurred. Disconnecting.')
    finally:
        dispatcher.unsubscribe(subscriber_id)
        rsock.close()
        logger.info('Removed subscriber %d.', subscriber_id)


def handle_client(sock: socket.socket, addr, dispatcher: MessageDispatcher, keep_alive: float):
    logger = logging.getLogger('handle_client,%s:%d' % addr)
    try:
        logger.info('Accept inbound connection from %s:%d.' % addr)

        if read_exactly(sock, 4) != b'PSMB':
            logger.info('Bad protocol magic.')
            return

        protocol = socket.ntohl(struct.unpack('I', read_exactly(sock, 4))[0])
        logger.info('Protocol version: %d', protocol)
        if protocol not in {1, 2}:
            logger.info(f'Unsupported protocol: {protocol}')
            sock.sendall(b'UNSUPPORTED PROTOCOL\0')
            return

        options = read_exactly(sock, 4)
        if options != b'\x00\x00\x00\x00':
            logger.info(f'Bad options: {options}')
            return

        sock.sendall(b'OK\0\x00\x00\x00\x00')
        logger.info('Complete handshaking.')

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
                logger.info(f'Switch to PUBLISH mode. Topic is {topic_id}.')
                _publish(sock, addr, dispatcher, topic_id, keep_alive, protocol=protocol)
                break
            elif mode == b'SUB':
                options = int.from_bytes(read_exactly(sock, 4), NETWORK_BYTEORDER, signed=False)
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
                logger.info(f'Switch to SUBSCRIBE mode. Subscriber is {subscriber_id}.')
                _subscribe(sock, addr, dispatcher, subscriber_id, id_pattern, keep_alive)
                break
            else:
                sock.sendall(b'BAD COMMAND\0')
                break
    except SubscriberAlreadyExistsError:
        logger.exception(f'Invalid client.')
    except (ConnectionError, IOError, IncompleteReadError):
        logger.exception('I/O error.')
    except Exception:
        logger.exception('Unexpected exception.')
    finally:
        sock.close()
        logger.info('Connection is closed.')
