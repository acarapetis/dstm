"""Tests for high-level tasks defined using the @task decorator and autowired imports"""

import sys
from random import choices
from string import ascii_lowercase

import pytest

from dstm.client.base import MessageClient
from dstm.exceptions import WiringError
from dstm.tasks.broker import TaskBroker


def test_direct_call_of_decorated_task(capfd):
    from tests.rabbit_city.names import name_rabbits

    name_rabbits(count=2)
    out, err = capfd.readouterr()
    assert out == "There are 2 rabbits and they're all called Peter\n"


def test_autowired_worker(client: MessageClient, capfd):
    prefix = "".join(choices(ascii_lowercase, k=10)) + "-"
    broker = TaskBroker(client=client, queue_prefix=prefix)
    broker.destroy_queues(["warren"])
    broker.create_queues(["warren"])

    from tests.rabbit_city.names import name_rabbits

    name_rabbits.submit_to(broker, count=3)

    # We had to import this to submit the task, let's pop it out of sys.modules to
    # check it's reimported again by the autowiring
    sys.modules.pop("tests.rabbit_city.names", None)
    assert "tests.rabbit_city.names" not in sys.modules

    broker.run_worker("warren", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "There are 3 rabbits and they're all called Peter\n"

    # The worker should have dynamically imported the module based on the task message
    assert "tests.rabbit_city.names" in sys.modules
    broker.destroy_queues(["warren"])


def test_autowired_worker_without_default_queue_error(client: MessageClient, capfd):
    prefix = "".join(choices(ascii_lowercase, k=10)) + "-"
    broker = TaskBroker(client=client, queue_prefix=prefix)
    from tests.rabbit_city.tasks import what_that_rabbit_do

    with pytest.raises(WiringError):
        broker.submit(what_that_rabbit_do, "peter")


def test_autowired_worker_with_default_queue(client: MessageClient, capfd):
    prefix = "".join(choices(ascii_lowercase, k=10)) + "-"
    broker = TaskBroker(client=client, default_queue="messages", queue_prefix=prefix)
    broker.destroy_queues(["messages"])
    broker.create_queues(["messages"])

    from tests.rabbit_city.tasks import what_that_rabbit_do

    broker.submit(what_that_rabbit_do, "peter")
    broker.run_worker("messages", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "peter digs holes.\n"

    broker.destroy_queues(["messages"])
