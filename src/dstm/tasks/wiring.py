from importlib import import_module
from typing import Generic, Protocol, TypeVar

from dstm.exceptions import WiringError
from dstm.tasks.types import TaskIdentity, TaskImpl

T = TypeVar("T", bound=TaskImpl)


class TaskWiring(Generic[T], Protocol):
    def func_to_identity(self, func: T) -> TaskIdentity: ...
    def name_to_func(self, task_name: str) -> T: ...


class AutoWiring(TaskWiring[T]):
    def func_to_identity(self, func: T) -> TaskIdentity:
        try:
            queue = getattr(func, "queue")
        except AttributeError as e:
            raise WiringError(
                "AutoWiring only works with @task-decorated functions"
            ) from e
        return TaskIdentity(f"{func.__module__}:{func.__name__}", queue)

    def name_to_func(self, task_name: str) -> T:
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

    def name_to_func(self, task_name: str) -> T:
        try:
            return self.funcs[task_name]
        except KeyError:
            raise WiringError(f"No task with name {task_name} found")

    def func_to_identity(self, func: T) -> TaskIdentity:
        try:
            return self.ids[func]
        except KeyError:
            raise WiringError(f"Function {func!r} not wired as a task")
