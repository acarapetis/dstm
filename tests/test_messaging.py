import pytest
from dstm.message import Message
from tests.conftest import ClientFactory


def test_send_receive(topic: str, make_client: ClientFactory):
    with make_client() as c:
        c.publish(topic, Message({"hello": "world"}))

    with make_client() as c:
        msg = next(c.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        c.ack(msg)


def test_requeue_then_ack(topic, make_client: ClientFactory):
    with make_client() as c:
        c.publish(topic, Message({"hello": "world"}))

    with make_client() as c:
        msg = next(c.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        c.requeue(msg)

    # After requeuing, message should be visible immediately
    with make_client() as c:
        msg = next(c.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        c.ack(msg)

    # After acking, no message should be visible
    with make_client() as c:
        with pytest.raises(StopIteration):
            msg = next(c.listen(topic, time_limit=0))


def test_autoexpire_then_ack(topic, make_client: ClientFactory):
    with make_client() as c:
        c.publish(topic, Message({"hello": "world"}))

    with make_client() as c:
        msg = next(c.listen(topic, time_limit=0))
        assert msg.body == {"hello": "world"}
        # Don't requeue, let the broker return it to the queue automatically

    # Message should become visible within 2 seconds
    with make_client() as c:
        msg = next(c.listen(topic, time_limit=2))
        assert msg.body == {"hello": "world"}
        c.ack(msg)

    # After acking, no message should be visible
    with make_client() as c:
        with pytest.raises(StopIteration):
            msg = next(c.listen(topic, time_limit=0))
