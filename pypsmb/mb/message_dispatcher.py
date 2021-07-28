import logging
import re
import socket
from typing import List, Tuple, Dict, Iterator


class SubscriberAlreadyExistsError(Exception):
    pass


class MessageDispatcher:
    # subscriber_id -> (pattern, lsock, inbox)
    subscriptions: Dict[int, Tuple[re.Pattern, socket.socket, List[Tuple[bytes, str]]]]

    def __init__(self):
        self.subscriptions = {}
        self.logger = logging.getLogger(type(self).__name__)

    def publish(self, message: bytes, topic: str):
        for subscriber_id, (pattern, lsock, inbox) in self.subscriptions.items():
            assert isinstance(pattern, re.Pattern)
            assert isinstance(lsock, socket.socket)
            if pattern.fullmatch(topic):
                self.logger.info('Dispatch message to subscriber with id %d.', subscriber_id)
                inbox.append((message, topic))
                try:
                    lsock.send(b'\x00')  # notify rsock
                except IOError as e:
                    self.logger.error('Cannot notify subscriber %d with pattern %s: %s',
                                      subscriber_id, pattern.pattern, e)

    def subscribe(self, subscriber_id: int, pattern: str) -> socket.socket:
        lsock, rsock = socket.socketpair()
        if subscriber_id in self.subscriptions:
            raise SubscriberAlreadyExistsError()
        self.subscriptions[subscriber_id] = re.compile(pattern), lsock, []
        return rsock

    def unsubscribe(self, subscriber_id: int):
        if subscriber_id not in self.subscriptions:
            raise ValueError(f'Subscriber with id `{subscriber_id}` does not exist')
        _, lsock, _ = self.subscriptions.pop(subscriber_id)
        lsock.close()

    def read_inbox(self, subscriber_id: int) -> Iterator[Tuple[bytes, str]]:
        _, _, inbox = self.subscriptions[subscriber_id]
        while inbox:
            yield inbox.pop(0)
