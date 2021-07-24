import re
import threading
from collections import defaultdict
from typing import List, Tuple, Dict, Iterator


class MessageDispatcher:
    subscriptions: List[Tuple[int, re.Pattern, threading.Event]]
    inbox: Dict[int, List[Tuple[bytes, str]]]  # subscriber_id -> (message, topic)

    def __init__(self):
        self.inbox = defaultdict(lambda: [])
        self.subscriptions = []

    def publish(self, message: bytes, topic: str):
        for subscriber_id, pattern, event in self.subscriptions:
            assert isinstance(pattern, re.Pattern)
            assert isinstance(event, threading.Event)
            if pattern.fullmatch(topic):
                self.inbox[subscriber_id].append((message, topic))
                event.set()

    def subscribe(self, subscriber_id: int, pattern: str) -> threading.Event:
        self.subscriptions.append((subscriber_id, re.compile(pattern), event := threading.Event()))
        return event

    def read_inbox(self, subscriber_id: int) -> Iterator[Tuple[bytes, str]]:
        inbox = self.inbox[subscriber_id]
        while inbox:
            yield inbox.pop(0)
