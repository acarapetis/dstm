import logging
from random import choices
from string import ascii_lowercase
from typing import Callable

import boto3
import pytest
from pika import ConnectionParameters, PlainCredentials

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
        visibility_timeout=0,
        short_poll_sleep_seconds=0.01,
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
def queue(client: MessageClient):
    yield from _test_queue(client)


QueueFactory = Callable[[], str]


@pytest.fixture()
def queue_factory(client: MessageClient) -> QueueFactory:
    def factory():
        return next(_test_queue(client))

    return factory


def _test_queue(client: MessageClient):
    t = "".join(choices(ascii_lowercase, k=10))
    with client:
        client.create_queue(t)
    yield t
    with client:
        client.destroy_queue(t)
