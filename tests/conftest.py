from typing import Callable
from pika import ConnectionParameters, PlainCredentials
import pytest
from random import choices
from string import ascii_lowercase

from dstm.client.amqp import AMQPClient
from dstm.client.base import MessageClient


@pytest.fixture()
def make_client():
    def maker():
        return AMQPClient(
            ConnectionParameters(
                "localhost", credentials=PlainCredentials("rabbit", "carrot")
            )
        )

    return maker


ClientFactory = Callable[[], MessageClient]


@pytest.fixture()
def topic(make_client: ClientFactory):
    t = "".join(choices(ascii_lowercase, k=10))
    c = make_client()
    c.connect()
    c.create_topic(t)
    c.disconnect()
    return t
