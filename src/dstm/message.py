from dataclasses import dataclass, field
from typing import Any


@dataclass
class Message:
    body: dict
    headers: dict[str, Any] = field(default_factory=dict)
    _id: Any = None
