import os
from abc import ABC, abstractmethod
from typing import Dict

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.nodes import (
    FromNode,
    JoinNode,
    WhereNode,
    GroupByNode,
    OrderByNode,
    LimitNode,
    UnionNode,
    ExceptNode,
    IntersectNode,
    HavingNode,
    OffsetNode,
    SelectNode,
)
from pysqlscribe.env_utils import str2bool
from pysqlscribe.exceptions import DialectValidationError
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
    SCALAR_IDENTIFIER_REGEX,
    EXPRESSION_IDENTIFIER_REGEX,
    CASE_IDENTIFIER_REGEX,
    ALIAS_REGEX,
    ALIAS_SPLIT_REGEX,
)
from pysqlscribe.renderers.base import Renderer


class Dialect(ABC):
    __escape_identifiers_enabled: bool = True
    placeholder_style: str = "qmark"

    def __init__(self):
        self._renderer = self.make_renderer()

    @abstractmethod
    def make_renderer(self) -> Renderer: ...

    def make_placeholder(self, index: int) -> str:
        """Return the placeholder text for the Nth (1-indexed) bound parameter."""
        if self.placeholder_style == "qmark":
            return "?"
        if self.placeholder_style == "numeric":
            return f"${index}"
        if self.placeholder_style == "named":
            return f":{index}"
        raise ValueError(f"Unknown placeholder_style: {self.placeholder_style}")

    def validate(self, current_node: Node, next_node: Node):
        valid_next = self.valid_node_transitions.get(type(current_node), ())
        if type(next_node) not in valid_next:
            raise DialectValidationError(
                f"{type(next_node).__name__} cannot follow {type(current_node).__name__}"
            )

    @property
    def valid_node_transitions(self) -> dict[type[Node], tuple[type[Node], ...]]:
        return {
            SelectNode: (FromNode,),
            FromNode: (
                JoinNode,
                WhereNode,
                GroupByNode,
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            JoinNode: (
                WhereNode,
                GroupByNode,
                OrderByNode,
                LimitNode,
                JoinNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            WhereNode: (
                GroupByNode,
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            OrderByNode: (LimitNode,),
            LimitNode: (OffsetNode,),
            GroupByNode: (
                HavingNode,
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            HavingNode: (
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            UnionNode: (
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            ExceptNode: (
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
            IntersectNode: (
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
            ),
        }

    def escape_identifier(self, identifier: str):
        if not self.escape_identifiers_enabled:
            return identifier
        return self._escape_identifier(identifier)

    @abstractmethod
    def _escape_identifier(self, identifier: str): ...

    def escape_value(self, value) -> str:
        """Render a literal value as SQL, with dialect-appropriate escaping."""
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (int, float)):
            return str(value)
        raise NotImplementedError(
            f"Unsupported value type for SQL literal: {type(value).__name__}"
        )

    def normalize_identifiers_args(self, *args) -> str:
        arg = args[0]
        if not isinstance(arg, (list, tuple)):
            arg = [arg]
        identifiers = []
        for identifier in arg:
            if hasattr(identifier, "to_identifier_sql"):
                identifiers.append(identifier.to_identifier_sql(self))
            else:
                identifiers.append(self.validate_identifier(str(identifier).strip()))

        return ", ".join(identifiers)

    def validate_identifier(self, identifier: str) -> str:
        if VALID_IDENTIFIER_REGEX.match(identifier):
            identifier = self.escape_identifier(identifier)
        elif (
            AGGREGATE_IDENTIFIER_REGEX.match(identifier)
            or SCALAR_IDENTIFIER_REGEX.match(identifier)
            or EXPRESSION_IDENTIFIER_REGEX.match(identifier)
            or CASE_IDENTIFIER_REGEX.match(identifier)
        ):
            identifier = identifier
        elif len(parts := ALIAS_SPLIT_REGEX.split(identifier, maxsplit=1)) == 2:
            base, alias = parts[0].strip(), parts[1].strip()
            if not ALIAS_REGEX.match(alias):
                raise ValueError(f"Invalid SQL alias: {alias}")
            identifier = self.validate_identifier(base)
            identifier = f"{identifier} AS {alias}"
        else:
            raise ValueError(f"Invalid SQL identifier: {identifier}")
        return identifier

    @property
    def escape_identifiers_enabled(self):
        if not str2bool(os.environ.get("PYSQLSCRIBE_ESCAPE_IDENTIFIERS", "true")):
            return False
        return self.__escape_identifiers_enabled

    @escape_identifiers_enabled.setter
    def escape_identifiers_enabled(self, value: bool):
        self.__escape_identifiers_enabled = value

    def render(self, node: Node, collector=None) -> str:
        return self._renderer.render(node, collector)


class DialectRegistry:
    dialects: Dict[str, type[Dialect]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(dialect_class: type[Dialect]) -> type[Dialect]:
            cls.dialects[key] = dialect_class
            return dialect_class

        return decorator

    @classmethod
    def get_dialect(cls, key: str) -> Dialect:
        return cls.dialects[key]()
