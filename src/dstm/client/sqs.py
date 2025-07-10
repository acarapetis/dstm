import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator, Iterable

from dstm.client.base import MessageClient, MessageConnection
from dstm.exceptions import PublishError
from dstm.message import Message

if TYPE_CHECKING:
    import mypy_boto3_sqs.client
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef

logger = logging.getLogger(__name__)


def _client():
    import boto3

    return boto3.client("sqs")


@dataclass
class SQSClient(MessageClient):
    """SQS client using boto3."""

    client: "mypy_boto3_sqs.client.SQSClient" = field(default_factory=_client)
    long_poll_time: int = 5
    max_messages_per_request: int = 1
    visibility_timeout: int = 30
    short_poll_sleep_seconds: float = 1

    def connect(self):
        return SQSConnection(self)


@dataclass
class SQSConnection(MessageConnection):
    client: SQSClient

    @property
    def botoc(self):
        return self.client.client

    def __repr__(self):
        return "SQSClient"

    def disconnect(self) -> None:
        pass  # No persistent connection required

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        pass

    def _get_queue_url(self, queue_name: str) -> str:
        try:
            response = self.botoc.get_queue_url(QueueName=queue_name)
        except self.botoc.exceptions.QueueDoesNotExist:
            response = self.botoc.create_queue(QueueName=queue_name)
        return response["QueueUrl"]

    def publish(self, message: Message) -> None:
        """Publish message to SQS queue."""
        if not self.botoc:
            raise ConnectionError("Not connected to SQS")

        try:
            queue_url = self._get_queue_url(message.queue)

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
            self.botoc.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes,
            )

            logger.debug(f"Published message to SQS queue: {message.queue}")
        except Exception as e:
            raise PublishError(f"Failed to publish message: {e}") from e

    def create_queue(self, queue: str) -> None:
        self.botoc.create_queue(QueueName=queue)
        logger.debug(f"Setting VisibilityTimeout={self.client.visibility_timeout}")
        self.botoc.set_queue_attributes(
            QueueUrl=self._get_queue_url(queue),
            Attributes={"VisibilityTimeout": str(self.client.visibility_timeout)},
        )

    def destroy_queue(self, queue: str) -> None:
        self.botoc.delete_queue(QueueUrl=self._get_queue_url(queue))

    def listen(
        self, queues: Iterable[str] | str, time_limit: int | None = None
    ) -> Generator[Message]:
        if not queues:
            return

        if isinstance(queues, str):
            queues = [queues]
        else:
            queues = list(queues)

        # Don't use long polling if we have multiple queues to check
        wait_time = self.client.long_poll_time if len(queues) == 1 else 0

        if time_limit is not None:
            end_time = time.monotonic() + time_limit
        else:
            end_time = None

        first = True
        while True:
            if end_time is not None:
                delta = end_time - time.monotonic()
                if delta <= 0:
                    if first:
                        logger.debug(
                            f"{queues=}, {time_limit=}; {delta=} but this is "
                            "the first loop, setting delta = 0"
                        )
                        delta = 0
                    else:
                        logger.debug(f"{queues=}, {time_limit=}; {delta=}, breaking")
                        break
                else:
                    logger.debug(f"{queues=}, {time_limit=}; {delta=}")
                if wait_time > delta:
                    wait_time = delta
            logger.debug(f"{queues=}, {time_limit=}; continuing with {int(wait_time)=}")
            first = False

            for queue in queues:
                queue_url = self._get_queue_url(queue)
                response = self.botoc.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=self.client.max_messages_per_request,
                    WaitTimeSeconds=int(wait_time),
                    MessageAttributeNames=["All"],
                )

                messages = response.get("Messages", [])
                response["ResponseMetadata"]

                for sqs_message in messages:
                    try:
                        attrs = sqs_message.get("MessageAttributes", {})
                        assert "Body" in sqs_message
                        assert "ReceiptHandle" in sqs_message
                        message = Message(
                            queue=queue,
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

            if len(queues) > 1:
                # When watching multiple queues we don't use long-polling, so we need to
                # manually sleep to avoid hammering the API
                time.sleep(self.client.short_poll_sleep_seconds)

    def ack(self, message: Message):
        self.botoc.delete_message(
            QueueUrl=message._id[0],
            ReceiptHandle=message._id[1],
        )

    def requeue(self, message: Message) -> None:
        self.botoc.change_message_visibility(
            QueueUrl=message._id[0],
            ReceiptHandle=message._id[1],
            VisibilityTimeout=0,
        )
