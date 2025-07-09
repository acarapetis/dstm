"""Tests for high-level tasks defined using the @task decorator and autowired imports"""

import sys
from dstm.task import autowire, run_worker
from tests.conftest import ClientFactory
from multiprocessing import Process


def test_direct_call_of_decorated_task(capfd):
    from tests.autowire_test_package.tasks import name_rabbits

    name_rabbits(count=2)
    out, err = capfd.readouterr()
    assert out == "There are 2 rabbits and they're all called Peter\n"


def test_autowired_worker(make_client: ClientFactory, capfd):
    # Another test might imported this in the same pytest process, so pop it if it's
    # there:
    sys.modules.pop("tests.autowire_test_package.tasks", None)

    # Submit job in another process so that the module isn't imported yet in this one
    def submit_job():
        from tests.autowire_test_package.tasks import name_rabbits

        with make_client() as c:
            name_rabbits.submit(c, count=3)

    proc = Process(target=submit_job)
    proc.start()
    proc.join()

    # The worker should now dynamically import the module based on the task message
    assert "tests.autowire_test_package.tasks" not in sys.modules
    with make_client() as c:
        run_worker(c, "warren", autowire, time_limit=0)
    assert "tests.autowire_test_package.tasks" in sys.modules

    out, err = capfd.readouterr()
    assert out == "There are 3 rabbits and they're all called Peter\n"
