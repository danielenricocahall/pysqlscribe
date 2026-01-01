from enum import Enum


class JoinType(str, Enum):
    INNER: str = "INNER"
    OUTER: str = "OUTER"
    LEFT: str = "LEFT"
    RIGHT: str = "RIGHT"
    CROSS: str = "CROSS"
    NATURAL: str = "NATURAL"

    def __str__(self):
        return self.value
