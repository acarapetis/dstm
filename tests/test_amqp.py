from dstm.message import Message


def test_send_receive(topic: str, make_client):
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


def test_acking(topic, make_client):
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
