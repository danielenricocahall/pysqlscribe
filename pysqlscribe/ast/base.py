from abc import ABC
from typing import Self, Any, Protocol, runtime_checkable

from pysqlscribe.exceptions import InvalidNodeException, DialectValidationError


class Node(ABC):
    next_: Self | None = None
    prev_: Self | None = None
    state: dict[str, Any]

    def __init__(self, state):
        self.state = state

    def add(self, next_: Self, dialect: "DialectProtocol") -> None:
        try:
            dialect.validate(self, next_)
        except DialectValidationError as e:
            raise InvalidNodeException(f"{type(dialect).__name__}: {e}") from e
        next_.prev_ = self
        self.next_ = next_


@runtime_checkable
class DialectProtocol(Protocol):
    def validate(self, current_node: Node, next_node: Node) -> None: ...
