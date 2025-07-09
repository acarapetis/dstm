import os

from dstm.client.base import MessageClient


def client_from_uri(uri: str) -> MessageClient:
    if uri.startswith("amqp://"):
        from dstm.client.amqp import AMQPClient
        from pika import URLParameters

        return AMQPClient(URLParameters(uri))
    if uri.startswith("sqs://"):
        import boto3
        from dstm.client.sqs import SQSClient

        return SQSClient(
            boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
        )
    raise ValueError(f"Unrecognized URI {uri}")
