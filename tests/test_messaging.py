import pytest
from dstm.client.base import MessageClient
from dstm.message import Message


def test_send_receive(topic: str, client: MessageClient):
    with client:
        client.publish(topic, Message({"hello": "world"}))

    with client:
        msg = next(client.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        client.ack(msg)


def test_requeue_then_ack(topic, client: MessageClient):
    with client:
        client.publish(topic, Message({"hello": "world"}))

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
        client.publish(topic, Message({"hello": "world"}))

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
