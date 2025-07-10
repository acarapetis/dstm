from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    queue: str
    body: T
    headers: dict[str, Any] = field(default_factory=dict)
    _id: Any = None
