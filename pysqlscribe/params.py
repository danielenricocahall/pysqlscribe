from typing import Any


class Literal:
    """A SQL literal value whose rendering is deferred until build time.

    During the default (inline) build path, a Literal renders as an
    escaped SQL constant via its dialect's escape_value. During the
    parameterized build path, it is appended to the ParamCollector and
    rendered as a dialect-appropriate placeholder.
    """

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self) -> str:
        return f"Literal({self.value!r})"


class ParamCollector:
    """Accumulates literal values and emits dialect-appropriate placeholders.

    Passed through Renderer / Expression render paths when build(parameterize=True).
    """

    def __init__(self, dialect):
        self.dialect = dialect
        self.params: list[Any] = []

    def add(self, value: Any) -> str:
        self.params.append(value)
        return self.dialect.make_placeholder(len(self.params))
