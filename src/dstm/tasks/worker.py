"""Run an autowired dstm worker."""

import logging
import time

from dstm.client.base import MessageClient
from dstm.tasks.types import TaskInstance
from dstm.tasks.wiring import TaskWiring

logger = logging.getLogger(__name__)


def run_task(instance: TaskInstance, wiring: TaskWiring):
    impl = wiring.get_task_by_name(instance.task_name)
    impl(*instance.args, **instance.kwargs)


def run_worker(
    client: MessageClient,
    queues: list[str],
    wiring: TaskWiring,
    time_limit: int | None = None,
    task_limit: int | None = None,
    raise_errors: bool = False,
):
    with client.connect() as conn:
        logger.info(f"Worker started using {client!r}, watching queues {queues}")
        for index, message in enumerate(conn.listen(queues, time_limit=time_limit)):
            instance = TaskInstance(**message.body)
            logger.info(f"{instance} received")
            try:
                t0 = time.perf_counter()
                run_task(instance, wiring)
                dt = time.perf_counter() - t0
            except Exception:
                if raise_errors:
                    raise
                logger.exception(f"{instance} failed, requeuing.")
                conn.requeue(message)
            else:
                logger.info(f"{instance} succeeded in {dt:.1e} seconds.")
                conn.ack(message)
            if task_limit is not None and index + 1 >= task_limit:
                logger.info(f"Worker hit task limit of {task_limit}, terminating.")
                break
