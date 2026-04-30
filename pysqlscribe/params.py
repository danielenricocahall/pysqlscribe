import datetime
import decimal
from typing import Any


def ansi_escape_value(value: Any) -> str:
    """Render a Python value as an ANSI SQL literal.

    Used as the default for ``Dialect.escape_value`` and as the no-dialect
    fallback in column expression rendering. Dialects override per type as
    needed (e.g. MySQL/SQLite/Oracle render bool as ``1``/``0``).

    isinstance ordering matters: ``bool`` is a subclass of ``int``, and
    ``datetime.datetime`` is a subclass of ``datetime.date``, so the more
    specific types are checked first.
    """
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, datetime.date):
        return "'" + value.isoformat() + "'"
    if value is None:
        return "NULL"
    # TODO: bytes literals are dialect-specific (E'\\x...' for postgres,
    # 0x... for mysql, X'...' for sqlite, HEXTORAW('...') for oracle); add
    # when a real use-case forces the per-dialect rendering.
    raise NotImplementedError(
        f"Unsupported value type for SQL literal: {type(value).__name__}"
    )


class Literal:
    """A SQL literal value whose rendering is deferred until build time.

    During the default (inline) build path, a Literal renders as an
    escaped SQL constant via its dialect's escape_value. During the
    parameterized build path, it is appended to the ParamCollector and
    rendered as a dialect-appropriate placeholder.
    """

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
