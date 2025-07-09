from typing import Callable
from pika import ConnectionParameters, PlainCredentials
import pytest
from random import choices
from string import ascii_lowercase
import logging
import boto3

from dstm.client.amqp import AMQPClient
from dstm.client.base import MessageClient
from dstm.client.sqs import SQSClient

# pika does a LOT of info-level logging we don't care about.
logging.getLogger("pika").setLevel(logging.WARNING)


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
def client(request):
    return {"sqs": make_sqs(), "amqp": make_amqp()}[request.param]


@pytest.fixture()
def topic(client: MessageClient):
    yield from _test_topic(client)


TopicFactory = Callable[[], str]


@pytest.fixture()
def topic_factory(client: MessageClient) -> TopicFactory:
    def factory():
        return next(_test_topic(client))

    return factory


def _test_topic(client: MessageClient):
    t = "".join(choices(ascii_lowercase, k=10))
    with client:
        client.create_topic(t)
    yield t
    with client:
        client.destroy_topic(t)
