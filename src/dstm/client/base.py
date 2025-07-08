from typing import Generator, Protocol

from dstm.message import Message


class MessageClient(Protocol):
    """Abstract base class for messaging clients."""

    def connect(self) -> None:
        """Establish connection to the messaging system."""
        ...

    def disconnect(self) -> None:
        """Close connection to the messaging system."""
        ...

    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a topic."""
        ...

    def listen(self, topic: str) -> Generator[Message]:
        """Listen for messages on a topic. Blocks while listening, then yields message contents and repeats."""
        ...

    def ack(self, message: Message) -> None:
        """Acknowledge that a message has been handled successfully."""
        ...
