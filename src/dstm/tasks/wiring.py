from importlib import import_module
from typing import Callable, ParamSpec, Protocol, TypeVar

from dstm.exceptions import WiringError

P = ParamSpec("P")
R = TypeVar("R")
TaskImpl = Callable[P, R]


class TaskWiring(Protocol):
    def func_to_name(self, func: TaskImpl) -> str: ...
    def name_to_func(self, task_name: str) -> TaskImpl: ...


class AutoWiring(TaskWiring):
    def func_to_name(self, func: TaskImpl) -> str:
        return f"{func.__module__}:{func.__name__}"

    def name_to_func(self, task_name: str) -> TaskImpl:
        module, name = task_name.split(":")
        return getattr(import_module(module), name)


class HardWiring(TaskWiring):
    mapping: dict[str, TaskImpl]
    inverse: dict[TaskImpl, str]

    def __init__(self, mapping: dict[str, TaskImpl]):
        self.mapping = mapping
        self.inverse = {v: k for k, v in mapping.items()}

    def name_to_func(self, task_name: str) -> TaskImpl:
        try:
            return self.mapping[task_name]
        except KeyError:
            raise WiringError(f"No task with name {task_name} found")

    def func_to_name(self, func: TaskImpl) -> str:
        try:
            return self.inverse[func]
        except KeyError:
            raise WiringError(f"Function {func!r} not wired as a task")
