from typing import Callable
from pika import ConnectionParameters, PlainCredentials
import pytest
from random import choices
from string import ascii_lowercase
import boto3

from dstm.client.amqp import AMQPClient
from dstm.client.sqs import SQSClient
from dstm.client.base import MessageClient


def make_sqs():
    return SQSClient(
        boto3.client(
            "sqs",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        ),
        visibility_timeout_for_new_queues=1,
    )


def make_amqp():
    return AMQPClient(
        ConnectionParameters(
            "localhost", credentials=PlainCredentials("rabbit", "carrot")
        )
    )


@pytest.fixture(params=["sqs", "amqp"])
def make_client(request):
    return {"sqs": make_sqs, "amqp": make_amqp}[request.param]


ClientFactory = Callable[[], MessageClient]


@pytest.fixture()
def topic(make_client: ClientFactory):
    t = "".join(choices(ascii_lowercase, k=10))
    c = make_client()
    c.connect()
    c.create_topic(t)
    c.disconnect()
    return t
