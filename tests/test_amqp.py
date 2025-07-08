from pika import ConnectionParameters, PlainCredentials
from random import choices
from string import ascii_lowercase

from dstm.client.amqp import AMQPClient
from dstm.message import Message


def make_client():
    return AMQPClient(
        ConnectionParameters(
            "localhost", credentials=PlainCredentials("rabbit", "carrot")
        )
    )


def test_send_receive():
    topic = "".join(choices(ascii_lowercase, k=10))

    c = make_client()
    c.connect()
    c.publish(topic, Message({"hello": "world"}))
    c.disconnect()

    c = make_client()
    c.connect()

    msg = next(c.listen(topic))
    assert msg.body == {"hello": "world"}
    c.ack(msg)
    c.disconnect()

    # with ProcessPoolExecutor(max_workers=2) as pool:
    #     pool.submit(send)
    #     assert pool.submit(receive).result(timeout=0.5).body == {"hello": "world"}


def test_acking():
    topic = "".join(choices(ascii_lowercase, k=10))

    c = make_client()
    c.connect()
    c.publish(topic, Message({"hello": "world"}))
    c.disconnect()

    c = make_client()
    c.connect()
    msg = next(c.listen(topic))
    assert msg.body == {"hello": "world"}
    c.disconnect()

    c.connect()
    msg = next(c.listen(topic))
    assert msg.body == {"hello": "world"}
    c.ack(msg)
    c.disconnect()
