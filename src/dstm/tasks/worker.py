"""Run an autowired dstm worker."""

import logging
import time
from typing import TypedDict

from dstm.client.base import MessageClient
from dstm.tasks.wiring import TaskWiring

logger = logging.getLogger(__name__)


class TaskInstance(TypedDict):
    task_name: str
    args: list | tuple
    kwargs: dict


def run_task(instance: TaskInstance, wiring: TaskWiring):
    impl = wiring.name_to_func(instance["task_name"])
    impl(*instance["args"], **instance["kwargs"])


def run_worker(
    client: MessageClient,
    queues: list[str],
    wiring: TaskWiring,
    time_limit: int | None = None,
    task_limit: int | None = None,
):
    with client.connect() as conn:
        logger.info(f"Worker started using {client!r}, watching queues {queues}")
        for index, message in enumerate(conn.listen(queues, time_limit=time_limit)):
            try:
                t0 = time.perf_counter()
                run_task(message.body, wiring)
                dt = time.perf_counter() - t0
            except Exception:
                logger.exception(
                    f"Error running task {message.body['task_name']}, requeuing."
                )
                conn.requeue(message)
            else:
                logger.info(
                    f"Task {message.body['task_name']} succeeded in {dt:.1e} seconds."
                )
                conn.ack(message)
            if task_limit is not None and index + 1 >= task_limit:
                logger.info(f"Worker hit task limit of {task_limit}, terminating.")
                break
