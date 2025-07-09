"""Tests for low-level (i.e. "hardwired") task functions"""

from dstm.task import TaskWiring, run_worker, submit_task
from tests.conftest import ClientFactory


outputs = []


def simple_task(name: str, count: int):
    for i in range(count):
        outputs.append(f"hi {name}")


wiring: TaskWiring = {"simple_task": simple_task}.__getitem__


def test_simple_task(topic: str, make_client: ClientFactory):
    with make_client() as c:
        submit_task(topic, "simple_task", c, "steve", 3)

    outputs.clear()
    with make_client() as c:
        run_worker(c, topic, wiring, time_limit=0)
    assert outputs == ["hi steve"] * 3
