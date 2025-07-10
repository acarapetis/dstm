import json
import logging
import time
from dataclasses import dataclass
from typing import Generator, Iterable

import pika
import pika.connection
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

from dstm.client.base import MessageClient, MessageConnection
from dstm.exceptions import PublishError
from dstm.message import Message

logger = logging.getLogger(__name__)


class AMQPConnection(MessageConnection):
    connection: BlockingConnection
    channel: BlockingChannel

    def __init__(self, parameters: pika.connection.Parameters):
        try:
            self.connection = BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.debug(f"Connected to AMQP broker {parameters}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to AMQP: {e}") from e

    def disconnect(self) -> None:
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.debug("Disconnected from AMQP broker {self.parameters}")

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        self.disconnect()

    def _assert_connected(self) -> tuple[BlockingConnection, BlockingChannel]:
        if (
            not self.connection
            or self.connection.is_closed
            or not self.channel
            or self.channel.is_closed
        ):
            raise ConnectionError("Not connected to AMQP broker")
        return self.connection, self.channel

    def create_queue(self, queue: str) -> None:
        connection, channel = self._assert_connected()
        channel.queue_declare(queue=queue, durable=True)

    def destroy_queue(self, queue: str) -> None:
        connection, channel = self._assert_connected()
        channel.queue_delete(queue=queue)

    def publish(self, message: Message) -> None:
        connection, channel = self._assert_connected()

        try:
            body = json.dumps(message.body)
            properties = pika.BasicProperties(
                headers=message.headers,
                delivery_mode=2,  # Make message persistent
            )

            channel.basic_publish(
                exchange="", routing_key=message.queue, body=body, properties=properties
            )

            logger.debug(f"Published message to queue: {message.queue}")
        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    def listen(
        self,
        queues: Iterable[str] | str,
        time_limit: float | None = None,
    ) -> Generator[Message]:
        connection, channel = self._assert_connected()

        if not queues:
            return

        if isinstance(queues, str):
            queues = [queues]

        if time_limit is not None:
            end_time = time.monotonic() + time_limit
        else:
            end_time = None

        # the basic_consume API is a bit awkward - we have to receive results via a
        # callback - so we set up this list to use as a temporary storage location
        responses: list[tuple] = []

        def store_response(ch, method, props, body):
            responses.append((queue, method, props, body))

        for queue in queues:
            channel.basic_consume(queue, store_response)

        first = True
        while True:
            if end_time is not None:
                delta = end_time - time.monotonic()
                if delta <= 0:
                    if first:
                        delta = 0
                    else:
                        break
            else:
                delta = None
            first = False

            connection.process_data_events(time_limit=delta)  # type: ignore

            for queue, method_frame, properties, body in responses:
                if method_frame is None:  # hit time limit
                    return
                try:
                    message = Message(
                        queue=queue,
                        body=json.loads(body.decode("utf-8")),
                        headers=properties.headers,
                        _id=method_frame.delivery_tag,
                    )
                except Exception as e:
                    logger.exception(f"Error parsing AMQP message: {e}")
                else:
                    yield message
            responses.clear()

    def ack(self, message: Message) -> None:
        connection, channel = self._assert_connected()
        channel.basic_ack(delivery_tag=message._id)

    def requeue(self, message: Message) -> None:
        connection, channel = self._assert_connected()
        channel.basic_nack(delivery_tag=message._id, requeue=True)


@dataclass
class AMQPClient(MessageClient):
    """AMQP client using pika."""

    parameters: pika.connection.Parameters

    def __repr__(self):
        return f"AMQPClient({self.parameters.host})"

    def connect(self) -> AMQPConnection:
        return AMQPConnection(self.parameters)
