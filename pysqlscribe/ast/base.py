from abc import ABC
from typing import Self, Any

from pysqlscribe.exceptions import InvalidNodeException, DialectValidationError
from pysqlscribe.protocols import DialectProtocol


class Node(ABC):
    next_: Self | None = None
    prev_: Self | None = None
    state: dict[str, Any]

    def __init__(self, state):
        self.state = state

    def add(self, next_: Self, dialect: DialectProtocol) -> None:
        try:
            dialect.validate(self, next_)
        except DialectValidationError as e:
            raise InvalidNodeException(f"{type(dialect).__name__}: {e}") from e
        next_.prev_ = self
        self.next_ = next_
