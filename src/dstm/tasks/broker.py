import logging
from dataclasses import dataclass, field
from typing import Iterable, ParamSpec

from dstm.client.base import MessageClient
from dstm.message import Message
from dstm.tasks.types import TaskImpl
from dstm.tasks.wiring import AutoWiring, TaskWiring
from dstm.tasks.worker import TaskInstance, run_worker

logger = logging.getLogger(__name__)


def submit_task(
    queue: str,
    task_name: str,
    client: MessageClient,
    /,
    *args,
    **kwargs,
) -> None:
    with client.connect() as conn:
        logger.info(f"Submitting {task_name=} to {queue=}")
        msg: Message[TaskInstance] = Message(
            queue,
            {
                "task_name": task_name,
                "args": args,
                "kwargs": kwargs,
            },
        )
        conn.publish(msg)


P = ParamSpec("P")


@dataclass
class TaskBroker:
    client: MessageClient
    queue_prefix: str = ""
    wiring: TaskWiring = field(default_factory=AutoWiring)

    def run_worker(
        self,
        queues: Iterable[str] | str,
        time_limit: int | None = None,
        task_limit: int | None = None,
    ):
        if isinstance(queues, str):
            queues = [queues]
        run_worker(
            client=self.client,
            queues=[self.queue_prefix + g for g in queues],
            wiring=self.wiring,
            time_limit=time_limit,
            task_limit=task_limit,
        )

    def create_queues(self, queues: Iterable[str] | str):
        if isinstance(queues, str):
            queues = [queues]
        with self.client.connect() as conn:
            for g in queues:
                conn.create_queue(self.queue_prefix + g)

    def destroy_queues(self, queues: Iterable[str] | str):
        if isinstance(queues, str):
            queues = [queues]
        with self.client.connect() as conn:
            for g in queues:
                conn.destroy_queue(self.queue_prefix + g)

    def submit(self, task: TaskImpl[P], /, *args: P.args, **kwargs: P.kwargs):
        task_id = self.wiring.func_to_identity(task)
        queue = self.queue_prefix + task_id.queue
        submit_task(queue, task_id.name, self.client, *args, **kwargs)
