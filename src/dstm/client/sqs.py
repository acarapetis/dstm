import json
import logging
import time
import typing
from dataclasses import dataclass

from dstm.client.base import MessageClient
from dstm.exceptions import PublishError
from dstm.message import Message

if typing.TYPE_CHECKING:
    import mypy_boto3_sqs.client
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef

logger = logging.getLogger(__name__)


@dataclass
class SQSClient(MessageClient):
    """SQS client using boto3."""

    client: "mypy_boto3_sqs.client.SQSClient"
    long_poll_time: int = 5
    max_messages_per_request: int = 1
    visibility_timeout_for_new_queues: int = 30

    def connect(self) -> None:
        pass  # No persistent connection required

    def disconnect(self) -> None:
        pass  # No persistent connection required

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        pass

    def _get_queue_url(self, queue_name: str) -> str:
        try:
            response = self.client.get_queue_url(QueueName=queue_name)
        except self.client.exceptions.QueueDoesNotExist:
            response = self.client.create_queue(QueueName=queue_name)
        return response["QueueUrl"]

    def publish(self, topic: str, message: Message) -> None:
        """Publish message to SQS queue."""
        if not self.client:
            raise ConnectionError("Not connected to SQS")

        try:
            queue_url = self._get_queue_url(topic)

            # Prepare message
            message_body = json.dumps(message.body)
            message_attributes: dict[str, "MessageAttributeValueTypeDef"] = {
                key: {
                    "StringValue": str(value),
                    "DataType": "String",
                }
                for key, value in message.headers.items()
            }

            # Send message
            self.client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes,
            )

            logger.debug(f"Published message to SQS queue: {topic}")
        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    def create_topic(self, topic: str) -> None:
        self.client.create_queue(QueueName=topic)
        self.client.set_queue_attributes(
            QueueUrl=self._get_queue_url(topic),
            Attributes={
                "VisibilityTimeout": str(self.visibility_timeout_for_new_queues)
            },
        )

    def listen(
        self,
        topic: str,
        time_limit: int | None = None,
    ) -> typing.Generator[Message]:
        t0 = time.monotonic()
        queue_url = self._get_queue_url(topic)
        wait_time = self.long_poll_time
        if time_limit is not None and time_limit < wait_time:
            wait_time = time_limit

        while True:
            response = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=self.max_messages_per_request,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])

            for sqs_message in messages:
                try:
                    attrs = sqs_message.get("MessageAttributes", {})
                    assert "Body" in sqs_message
                    assert "ReceiptHandle" in sqs_message
                    message = Message(
                        body=json.loads(sqs_message["Body"]),
                        headers={
                            k: v["StringValue"]
                            for k, v in attrs.items()
                            if "StringValue" in v
                        },
                        _id=(queue_url, sqs_message["ReceiptHandle"]),
                    )
                except Exception as e:
                    logger.exception(f"Error parsing SQS message: {e}")
                else:
                    yield message

            if time_limit is not None and time.monotonic() > t0 + time_limit:
                break

    def ack(self, message: Message):
        self.client.delete_message(
            QueueUrl=message._id[0],
            ReceiptHandle=message._id[1],
        )

    def requeue(self, message: Message) -> None:
        self.client.change_message_visibility(
            QueueUrl=message._id[0],
            ReceiptHandle=message._id[1],
            VisibilityTimeout=0,
        )
