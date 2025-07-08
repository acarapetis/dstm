from dataclasses import dataclass
from typing import Any


@dataclass
class Message:
    body: dict
    headers: dict[str, Any]
    _id: Any
