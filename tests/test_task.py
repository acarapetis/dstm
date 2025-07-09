"""Tests for low-level task functions with hardwired name->implementation mappings and
explicit topics"""

from dstm.client.base import MessageClient
from dstm.task import TaskWiring, run_worker, submit_task


outputs = []


def simple_task(name: str, count: int):
    for i in range(count):
        outputs.append(f"hi {name}")


wiring: TaskWiring = {"simple_task": simple_task}.__getitem__


def test_simple_task(topic: str, client: MessageClient):
    submit_task(topic, "simple_task", client, "steve", 3)

    outputs.clear()
    run_worker(client, topic, wiring, time_limit=0)
    assert outputs == ["hi steve"] * 3
