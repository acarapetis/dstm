from dataclasses import dataclass
from importlib import import_module
from typing import Generic, Protocol, TypeVar

from dstm.exceptions import WiringError
from dstm.tasks.types import TaskFunc, TaskIdentity

T = TypeVar("T", bound=TaskFunc)


class TaskWiring(Generic[T], Protocol):
    def get_task_identity(self, func: T) -> TaskIdentity: ...
    def get_task_by_name(self, task_name: str) -> T: ...


@dataclass
class AutoWiring(TaskWiring[T]):
    default_queue: str | None = None

    def get_task_identity(self, func: T) -> TaskIdentity:
        queue = getattr(func, "queue", self.default_queue)
        if queue is None:
            raise WiringError(
                "AutoWiring without default_queue only works with "
                "@task-decorated functions"
            )
        return TaskIdentity(f"{func.__module__}:{func.__name__}", queue)

    def get_task_by_name(self, task_name: str) -> T:
        module, name = task_name.split(":")
        return getattr(import_module(module), name)


class HardWiring(TaskWiring[T]):
    funcs: dict[str, T]
    ids: dict[T, TaskIdentity]

    def __init__(self, mapping: dict[str, dict[str, T]]):
        self.funcs = {
            name: func
            for queue, submap in mapping.items()
            for name, func in submap.items()
        }
        self.ids = {
            func: TaskIdentity(name, queue)
            for queue, submap in mapping.items()
            for name, func in submap.items()
        }

    def get_task_by_name(self, task_name: str) -> T:
        try:
            return self.funcs[task_name]
        except KeyError:
            raise WiringError(f"No task with name {task_name} found")

    def get_task_identity(self, func: T) -> TaskIdentity:
        try:
            return self.ids[func]
        except KeyError:
            raise WiringError(f"Function {func!r} not wired as a task")
