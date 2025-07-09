from dataclasses import dataclass
from typing import Callable, Generic, ParamSpec, TypeVar, TypedDict
from importlib import import_module
from dstm.client.base import MessageClient
from dstm.message import Message
import logging

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
class TaskWrapper(Generic[P, R]):
    """Function wrapper that attaches the necessary metadata to make it a task, and a
    convenience method to submit an instance of this task."""

    func: Callable[P, R]
    topic: str
    task_name: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    def submit(self, client: MessageClient, /, *args: P.args, **kwargs: P.kwargs):
        submit_task(self.topic, self.task_name, client, *args, **kwargs)


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
            logger.warning(message.body)
            run_task(message.body, wiring)
            if task_limit is not None and index + 1 >= task_limit:
                logger.info(f"Worker hit task limit of {task_limit}, terminating.")
                break
        except Exception:
            logger.exception(f"Error running task {message.body['task_name']}")
        else:
            client.ack(message)
