"""Tests for high-level tasks defined using the @task decorator and autowired imports"""

import sys
from dstm.client.base import MessageClient
from dstm.task import TaskBackend, autowire
from multiprocessing import Process


def test_direct_call_of_decorated_task(capfd):
    from tests.autowire_test_package.tasks import name_rabbits

    name_rabbits(count=2)
    out, err = capfd.readouterr()
    assert out == "There are 2 rabbits and they're all called Peter\n"


def test_autowired_worker(client: MessageClient, capfd):
    # Another test might imported this in the same pytest process, so pop it if it's
    # there:
    sys.modules.pop("tests.autowire_test_package.tasks", None)
    backend = TaskBackend("prod-", autowire, client)
    backend.create_topics(["warren"])

    # Submit job in another process so that the module isn't imported yet in this one
    def submit_job():
        from tests.autowire_test_package.tasks import name_rabbits

        name_rabbits.submit_to(backend, count=3)

    proc = Process(target=submit_job)
    proc.start()
    proc.join()

    assert "tests.autowire_test_package.tasks" not in sys.modules

    backend.run_worker("warren", time_limit=0)

    out, err = capfd.readouterr()
    assert out == "There are 3 rabbits and they're all called Peter\n"

    # The worker have dynamically imported the module based on the task message
    assert "tests.autowire_test_package.tasks" in sys.modules
