"""Tests for hardwired plain-function tasks with TaskBackend"""

from random import choices
from string import ascii_lowercase

from dstm.client.base import MessageClient
from dstm.tasks.backend import TaskBackend


def test_direct_call_of_undecorated_task(capfd):
    from tests.rabbit_city.tasks import where_that_rabbit_live

    assert where_that_rabbit_live("jessica") == "hole"
    out, err = capfd.readouterr()
    assert out == "jessica lives in a hole.\n"


def test_hardwired_worker(client: MessageClient, capfd):
    from tests.rabbit_city.tasks import what_that_rabbit_do, wiring

    prefix = "".join(choices(ascii_lowercase, k=10))
    backend = TaskBackend(client, prefix, wiring)
    backend.destroy_queues(["rabbits"])
    backend.create_queues(["rabbits"])

    backend.submit(what_that_rabbit_do, "peter")

    backend.run_worker("rabbits", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "peter digs holes.\n"

    backend.destroy_queues(["rabbits"])
