from typing import Generator, Protocol, TypeVar

from dstm.message import Message

Self = TypeVar("Self", bound="MessageClient")


class MessageClient(Protocol):
    """Abstract base class for messaging clients."""

    def __enter__(self: Self) -> Self: ...

    def __exit__(self, type_, value, tb): ...

    def connect(self) -> None:
        """Establish connection to the messaging system."""
        ...

    def disconnect(self) -> None:
        """Close connection to the messaging system."""
        ...

    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a topic."""
        ...

    def listen(self, topic: str, time_limit: int | None = None) -> Generator[Message]:
        """Listen for messages on a topic. Blocks while listening, then yields message contents and repeats."""
        ...

    def ack(self, message: Message) -> None:
        """Acknowledge that a message has been handled successfully."""
        ...

    def requeue(self, message: Message) -> None:
        """Tell the broker that a message should be requeued."""
        ...

    def create_topic(self, topic: str) -> None:
        """Create a topic if it does not already exist."""
        ...
