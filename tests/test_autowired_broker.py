"""Tests for high-level tasks defined using the @task decorator and autowired imports"""

import sys
from random import choices
from string import ascii_lowercase

from dstm.client.base import MessageClient
from dstm.tasks.broker import TaskBroker


def test_direct_call_of_decorated_task(capfd):
    from tests.rabbit_city.names import name_rabbits

    name_rabbits(count=2)
    out, err = capfd.readouterr()
    assert out == "There are 2 rabbits and they're all called Peter\n"


def test_autowired_worker(client: MessageClient, capfd):
    prefix = "".join(choices(ascii_lowercase, k=10))
    broker = TaskBroker(client, prefix)
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
