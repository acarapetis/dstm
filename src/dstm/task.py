import logging
import time
from dataclasses import dataclass
from importlib import import_module
from typing import Callable, Generic, ParamSpec, TypedDict, TypeVar

from dstm.client.base import MessageClient
from dstm.message import Message

logger = logging.getLogger(__name__)


class TaskInstance(TypedDict):
    task_name: str
    args: list | tuple
    kwargs: dict


P = ParamSpec("P")
R = TypeVar("R")
TaskImpl = Callable[P, R]
TaskWiring = Callable[[str], TaskImpl]


def autowire(task_name: str) -> TaskImpl:
    """Automatic wiring that assumes task names are of the form "{module}:{function}"
    and lazily autoimports the corresponding modules."""
    module, name = task_name.split(":")
    return getattr(import_module(module), name)


def submit_task(
    topic: str,
    task_name: str,
    client: MessageClient,
    /,
    *args,
    **kwargs,
) -> None:
    client.create_topic(topic)
    msg: Message[TaskInstance] = Message(
        {
            "task_name": task_name,
            "args": args,
            "kwargs": kwargs,
        }
    )
    client.publish(topic, msg)


@dataclass
class TaskBackend:
    topic_prefix: str
    wiring: TaskWiring
    client: MessageClient

    def run_worker(
        self,
        task_group: str,
        time_limit: int | None = None,
        task_limit: int | None = None,
    ):
        run_worker(
            client=self.client,
            topic=self.topic_prefix + task_group,
            wiring=self.wiring,
            time_limit=time_limit,
            task_limit=task_limit,
        )


@dataclass
class TaskWrapper(Generic[P, R]):
    """Function wrapper that attaches the necessary metadata to make it a task, and a
    convenience method to submit an instance of this task."""

    func: Callable[P, R]
    task_group: str
    task_name: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    def submit_to(self, backend: TaskBackend, /, *args: P.args, **kwargs: P.kwargs):
        topic = backend.topic_prefix + self.task_group
        submit_task(topic, self.task_name, backend.client, *args, **kwargs)


def task(topic: str, task_name: str | None = None):
    def decorator(func: Callable[P, R]) -> TaskWrapper[P, R]:
        if task_name is None:
            name = f"{func.__module__}:{func.__name__}"
        else:
            name = task_name
        return TaskWrapper(func, topic, name)

    return decorator


def run_task(instance: TaskInstance, wiring: TaskWiring):
    impl = wiring(instance["task_name"])
    impl(*instance["args"], **instance["kwargs"])


def run_worker(
    client: MessageClient,
    topic: str,
    wiring: TaskWiring,
    time_limit: int | None = None,
    task_limit: int | None = None,
):
    for index, message in enumerate(client.listen(topic, time_limit=time_limit)):
        try:
            t0 = time.perf_counter()
            run_task(message.body, wiring)
            runtime = time.perf_counter() - t0
        except Exception:
            logger.exception(
                f"Error running task {message.body['task_name']}, requeuing."
            )
            client.requeue(message)
        else:
            logger.info(
                f"Task {message.body['task_name']} succeeded in {runtime:.1e} seconds."
            )
            client.ack(message)
        if task_limit is not None and index + 1 >= task_limit:
            logger.info(f"Worker hit task limit of {task_limit}, terminating.")
            break
