import pytest
import time
from dstm.client.base import MessageClient
from dstm.message import Message
from tests.conftest import TopicFactory


def test_send_receive(topic: str, client: MessageClient):
    with client:
        client.publish(Message(topic, {"hello": "world"}))

    with client:
        msg = next(client.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        client.ack(msg)


def test_timeout(topic, client: MessageClient):
    with client:
        t0 = time.monotonic()
        with pytest.raises(StopIteration):
            next(client.listen(topic, time_limit=1))
        t1 = time.monotonic()

        assert 0.9 < t1 - t0 < 1.1


def test_requeue_then_ack(topic, client: MessageClient):
    with client:
        client.publish(Message(topic, {"hello": "world"}))

    with client:
        msg = next(client.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        client.requeue(msg)

    # After requeuing, message should be visible immediately
    with client:
        msg = next(client.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        client.ack(msg)

    # After acking, no message should be visible
    with client:
        with pytest.raises(StopIteration):
            msg = next(client.listen(topic, time_limit=0))


def test_autoexpire_then_ack(topic, client: MessageClient):
    with client:
        client.publish(Message(topic, {"hello": "world"}))

    with client:
        msg = next(client.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        # Don't requeue, let the broker return it to the queue automatically

    # Message should become visible within 2 seconds
    with client:
        msg = next(client.listen(topic, time_limit=2))
        assert msg.body == {"hello": "world"}
        client.ack(msg)

    # After acking, no message should be visible
    with client:
        with pytest.raises(StopIteration):
            msg = next(client.listen(topic, time_limit=0))


def test_multiple_topics(topic_factory: TopicFactory, client: MessageClient):
    gaia = topic_factory()
    hades = topic_factory()

    with client:
        client.publish(Message(gaia, {"msg": "hello world"}))
        client.publish(Message(hades, {"msg": "hello underworld"}))

    with client:
        msgs = list(client.listen([gaia, hades], time_limit=0))
        assert {msg.body["msg"] for msg in msgs} == {"hello world", "hello underworld"}
        for msg in msgs:
            client.ack(msg)
