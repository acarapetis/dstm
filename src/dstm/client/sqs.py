import json
import logging
import typing

from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef

from dstm.client.base import MessageClient
from dstm.exceptions import PublishError
from dstm.message import Message

if typing.TYPE_CHECKING:
    import mypy_boto3_sqs.client

logger = logging.getLogger(__name__)


class SQSClient(MessageClient):
    """SQS client using boto3."""

    sqs: "mypy_boto3_sqs.client.SQSClient"

    def __init__(self, client: "mypy_boto3_sqs.client.SQSClient"):
        self.sqs = client
        self._consuming = False

    def connect(self) -> None:
        pass  # No persistent connection required

    def disconnect(self) -> None:
        pass  # No persistent connection required

    def _get_queue_url(self, queue_name: str) -> str:
        """Get or create queue URL."""
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
        except self.sqs.exceptions.QueueDoesNotExist:
            response = self.sqs.create_queue(QueueName=queue_name)
        return response["QueueUrl"]

    def publish(self, topic: str, message: Message) -> None:
        """Publish message to SQS queue."""
        if not self.sqs:
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
            self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes,
            )

            logger.debug(f"Published message to SQS queue: {topic}")
        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    def listen(
        self,
        topic: str,
        wait_time: int = 5,
        max_messages: int = 10,
        **_options,
    ) -> typing.Generator[Message]:
        queue_url = self._get_queue_url(topic)

        while True:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
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

    def ack(self, message: Message):
        # Delete message from queue
        self.sqs.delete_message(
            QueueUrl=message._id[0],
            ReceiptHandle=message._id[1],
        )
