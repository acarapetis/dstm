import json
import logging
from typing import Generator
import pika

from dstm.client.base import MessageClient
from dstm.exceptions import PublishError
from dstm.message import Message

logger = logging.getLogger(__name__)


class AMQPClient(MessageClient):
    """AMQP client using pika."""

    def __init__(self, parameters: pika.ConnectionParameters):
        self.parameters = parameters
        self.connection = None
        self.channel = None

    def connect(self) -> None:
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            logger.debug(f"Connected to AMQP broker {self.parameters}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to AMQP: {e}") from e

    def disconnect(self) -> None:
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.debug("Disconnected from AMQP broker {self.parameters}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_, value, tb):
        self.disconnect()

    def create_topic(self, topic: str) -> None:
        if not self.channel:
            raise ConnectionError("Not connected to AMQP broker")
        self.channel.queue_declare(queue=topic, durable=True)

    def publish(self, topic: str, message: Message) -> None:
        if not self.channel:
            raise ConnectionError("Not connected to AMQP broker")

        try:
            body = json.dumps(message.body)
            properties = pika.BasicProperties(
                headers=message.headers,
                delivery_mode=2,  # Make message persistent
            )

            self.channel.basic_publish(
                exchange="", routing_key=topic, body=body, properties=properties
            )

            logger.debug(f"Published message to queue: {topic}")
        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    def listen(
        self,
        topic: str,
        time_limit: int | None = None,
    ) -> Generator[Message]:
        if not self.channel:
            raise ConnectionError("Not connected to AMQP broker")

        for method_frame, properties, body in self.channel.consume(
            queue=topic, inactivity_timeout=time_limit
        ):
            if method_frame is None:  # hit time limit
                break
            try:
                message = Message(
                    body=json.loads(body.decode("utf-8")),
                    headers=properties.headers,
                    _id=method_frame.delivery_tag,
                )
            except Exception as e:
                logger.exception(f"Error parsing AMQP message: {e}")
            else:
                yield message

    def ack(self, message: Message) -> None:
        if not self.channel:
            raise ConnectionError("Not connected to AMQP broker")
        self.channel.basic_ack(delivery_tag=message._id)

    def requeue(self, message: Message) -> None:
        if not self.channel:
            raise ConnectionError("Not connected to AMQP broker")
        self.channel.basic_nack(delivery_tag=message._id, requeue=True)
