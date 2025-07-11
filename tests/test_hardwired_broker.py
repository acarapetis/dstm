"""Tests for hardwired plain-function tasks with TaskBroker"""

from random import choices
from string import ascii_lowercase

from dstm.client.base import MessageClient
from dstm.tasks.broker import TaskBroker


def test_direct_call_of_undecorated_task(capfd):
    from tests.rabbit_city.tasks import where_that_rabbit_live

    assert where_that_rabbit_live("jessica") == "hole"
    out, err = capfd.readouterr()
    assert out == "jessica lives in a hole.\n"


def test_hardwired_worker(client: MessageClient, capfd):
    from tests.rabbit_city.tasks import what_that_rabbit_do, wiring

    prefix = "".join(choices(ascii_lowercase, k=10))
    broker = TaskBroker(client, wiring, queue_prefix=prefix)
    broker.destroy_queues(["rabbits"])
    broker.create_queues(["rabbits"])

    broker.submit(what_that_rabbit_do, "peter")

    broker.run_worker("rabbits", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "peter digs holes.\n"

    broker.destroy_queues(["rabbits"])
