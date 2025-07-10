from typing import Generator, Iterable, Protocol, TypeVar

from dstm.message import Message

TConn = TypeVar("TConn", bound="MessageConnection")


class MessageConnection(Protocol):
    def __enter__(self: TConn) -> TConn: ...
    def __exit__(self, type_, value, tb): ...

    def publish(self, message: Message) -> None:
        """Publish a message to a queue."""
        ...

    def listen(
        self, queues: Iterable[str] | str, time_limit: int | None = None
    ) -> Generator[Message]:
        """Listen for messages on one or more queues. Blocks while listening, then
        yields message contents and repeats."""
        ...

    def ack(self, message: Message) -> None:
        """Acknowledge that a message has been handled successfully."""
        ...

    def requeue(self, message: Message) -> None:
        """Tell the broker that a message should be requeued."""
        ...

    def create_queue(self, queue: str) -> None:
        """Create a queue if it does not already exist."""
        ...

    def destroy_queue(self, queue: str) -> None:
        """Delete an existing queue."""
        ...


class MessageClient(Protocol):
    def connect(self) -> MessageConnection:
        """Establish connection to the messaging system."""
        ...
