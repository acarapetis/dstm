import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, ParamSpec

from dstm.client.base import MessageClient
from dstm.message import Message
from dstm.tasks.task import TaskWrapper
from dstm.tasks.wiring import AutoWiring, TaskWiring
from dstm.tasks.worker import TaskInstance, run_worker

logger = logging.getLogger(__name__)


def submit_task(
    topic: str,
    task_name: str,
    client: MessageClient,
    /,
    *args,
    **kwargs,
) -> None:
    with client:
        logger.info(f"Submitting {task_name=} to {topic=}")
        msg: Message[TaskInstance] = Message(
            topic,
            {
                "task_name": task_name,
                "args": args,
                "kwargs": kwargs,
            },
        )
        client.publish(msg)


P = ParamSpec("P")


@dataclass
class TaskBackend:
    client: MessageClient
    topic_prefix: str = ""
    wiring: TaskWiring = field(default_factory=AutoWiring)

    def run_worker(
        self,
        task_groups: Iterable[str] | str,
        time_limit: int | None = None,
        task_limit: int | None = None,
    ):
        if isinstance(task_groups, str):
            task_groups = [task_groups]
        run_worker(
            client=self.client,
            topics=[self.topic_prefix + g for g in task_groups],
            wiring=self.wiring,
            time_limit=time_limit,
            task_limit=task_limit,
        )

    def create_topics(self, task_groups: Iterable[str] | str):
        if isinstance(task_groups, str):
            task_groups = [task_groups]
        with self.client:
            for g in task_groups:
                self.client.create_topic(self.topic_prefix + g)

    def destroy_topics(self, task_groups: Iterable[str] | str):
        if isinstance(task_groups, str):
            task_groups = [task_groups]
        with self.client:
            for g in task_groups:
                self.client.destroy_topic(self.topic_prefix + g)

    def submit(self, task: TaskWrapper[P, Any], /, *args: P.args, **kwargs: P.kwargs):
        task_name = self.wiring.func_to_name(task)
        topic = self.topic_prefix + task.task_group
        submit_task(topic, task_name, self.client, *args, **kwargs)
