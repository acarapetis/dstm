"""Tests for low-level task functions (without using the TaskBroker)"""

from dstm.client.base import MessageClient
from dstm.tasks.broker import submit_task
from dstm.tasks.wiring import HardWiring
from dstm.tasks.worker import run_worker

outputs = []


def simple_task(name: str, count: int):
    for i in range(count):
        outputs.append(f"hi {name}")


wiring = HardWiring({"default": {"simple_task": simple_task}})


def test_simple_task(queue: str, client: MessageClient):
    submit_task(queue, "simple_task", client, "steve", 3)

    outputs.clear()
    run_worker(client, [queue], wiring, time_limit=0, raise_errors=True)
    assert outputs == ["hi steve"] * 3
