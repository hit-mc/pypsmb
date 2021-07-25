import re
import socket
from collections import defaultdict
from typing import List, Tuple, Dict, Iterator


class MessageDispatcher:
    subscriptions: List[Tuple[int, re.Pattern, socket.socket]]
    inbox: Dict[int, List[Tuple[bytes, str]]]  # subscriber_id -> (message, topic)

    def __init__(self):
        self.inbox = defaultdict(lambda: [])
        self.subscriptions = []

    def publish(self, message: bytes, topic: str):
        for subscriber_id, pattern, lsock in self.subscriptions:
            assert isinstance(pattern, re.Pattern)
            assert isinstance(lsock, socket.socket)
            if pattern.fullmatch(topic):
                self.inbox[subscriber_id].append((message, topic))
                lsock.send(b'\x00')  # notify rsock

    def subscribe(self, subscriber_id: int, pattern: str) -> socket.socket:
        lsock, rsock = socket.socketpair()
        self.subscriptions.append((subscriber_id, re.compile(pattern), lsock))
        return rsock

    def unsubscribe(self, subscriber_id: int):
        for ele in self.subscriptions:
            _id, _, lsock = ele
            if _id == subscriber_id:
                lsock.close()
                self.subscriptions.remove(ele)
                break

    def read_inbox(self, subscriber_id: int) -> Iterator[Tuple[bytes, str]]:
        inbox = self.inbox[subscriber_id]
        while inbox:
            yield inbox.pop(0)
