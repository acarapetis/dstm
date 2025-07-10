import logging
from dataclasses import dataclass
from functools import update_wrapper
from typing import TYPE_CHECKING, Callable, Generic, ParamSpec, TypeVar

from dstm.tasks.types import TaskImpl

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dstm.tasks.broker import TaskBroker


P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class TaskWrapper(Generic[P, R], TaskImpl[P]):
    """Function wrapper that attaches the necessary metadata to make it a task, and a
    convenience method to submit an instance of this task."""

    func: Callable[P, R]
    queue: str
    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    def submit_to(self, broker: "TaskBroker", /, *args: P.args, **kwargs: P.kwargs):
        """Alternative phrasing of backend.submit(self, ...)."""
        broker.submit(self, *args, **kwargs)


def task(queue: str):
    def decorator(func: Callable[P, R]) -> TaskWrapper[P, R]:
        wrapper = TaskWrapper(func, queue, __name__=func.__name__)
        update_wrapper(wrapper, func)
        return wrapper

    return decorator
