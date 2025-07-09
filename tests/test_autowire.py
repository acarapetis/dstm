"""Tests for high-level tasks defined using the @task decorator and autowired imports"""

from random import choices
from string import ascii_lowercase
import sys
from dstm.client.base import MessageClient
from dstm.tasks.backend import TaskBackend


def test_direct_call_of_decorated_task(capfd):
    from tests.autowire_test_package.tasks import name_rabbits

    name_rabbits(count=2)
    out, err = capfd.readouterr()
    assert out == "There are 2 rabbits and they're all called Peter\n"


def test_autowired_worker(client: MessageClient, capfd):
    prefix = "".join(choices(ascii_lowercase, k=10))
    backend = TaskBackend(client, prefix)
    backend.destroy_topics(["warren"])
    backend.create_topics(["warren"])

    from tests.autowire_test_package.tasks import name_rabbits

    name_rabbits.submit_to(backend, count=3)

    # We had to import this to submit the task, let's pop it out of sys.modules to
    # check it's reimported again by the autowiring
    sys.modules.pop("tests.autowire_test_package.tasks", None)
    assert "tests.autowire_test_package.tasks" not in sys.modules

    backend.run_worker("warren", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "There are 3 rabbits and they're all called Peter\n"

    # The worker should have dynamically imported the module based on the task message
    assert "tests.autowire_test_package.tasks" in sys.modules
    backend.destroy_topics(["warren"])
