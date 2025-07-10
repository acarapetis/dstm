from dataclasses import dataclass
from typing import Any, Generic, ParamSpec, Protocol

P = ParamSpec("P")


class TaskImpl(Generic[P], Protocol):
    """Either a (non-anonymous) python function, or the result of wrapping one with
    @dstm.tasks.task.task."""

    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any: ...


@dataclass
class TaskIdentity:
    name: str
    queue: str
