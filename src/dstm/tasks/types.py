import uuid
from dataclasses import dataclass, field
from typing import Any, Generic, ParamSpec, Protocol

P = ParamSpec("P")


class TaskFunc(Generic[P], Protocol):
    """Either a (non-anonymous) python function, or the result of wrapping one with
    @dstm.tasks.task.task."""

    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any: ...


@dataclass
class TaskIdentity:
    name: str
    queue: str


@dataclass
class TaskInstance:
    task_name: str
    args: list | tuple
    kwargs: dict
    task_instance_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self):
        return f"TaskInstance({self.task_name},{self.task_instance_id})"
