import logging
from typing import Iterable, ParamSpec

from dstm.client.base import MessageClient
from dstm.message import Message
from dstm.tasks.types import TaskFunc
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


class TaskBroker:
    client: MessageClient
    wiring: TaskWiring
    queue_prefix: str

    def __init__(
        self,
        client: MessageClient,
        wiring: TaskWiring | None = None,
        queue_prefix: str = "",
        default_queue: str | None = None,
    ) -> None:
        if wiring is None:
            self.wiring = AutoWiring(default_queue=default_queue)
        else:
            self.wiring = wiring
            if default_queue is not None:
                raise ValueError("Cannot provide both `wiring` and `default_queue`")
        self.client = client
        self.queue_prefix = queue_prefix

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

    def submit(self, task: TaskFunc[P], /, *args: P.args, **kwargs: P.kwargs):
        task_id = self.wiring.func_to_identity(task)
        queue = self.queue_prefix + task_id.queue
        submit_task(queue, task_id.name, self.client, *args, **kwargs)
