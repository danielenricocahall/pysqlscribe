import os
from abc import ABC, abstractmethod

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
    InsertNode,
    ReturningNode,
)
from pysqlscribe.env_utils import str2bool
from pysqlscribe.exceptions import DialectValidationError
from pysqlscribe.regex_patterns import (
    ALIAS_SPLIT_REGEX,
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
    SCALAR_IDENTIFIER_REGEX,
    EXPRESSION_IDENTIFIER_REGEX,
    ALIAS_REGEX,
)


class Dialect(ABC):
    __escape_identifiers_enabled: bool = True

    def validate(self, current_node: Node, next_node: Node):
        valid_next = self.valid_node_transitions.get(type(current_node), ())
        if not isinstance(next_node, valid_next):
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
                OffsetNode,
            ),
            JoinNode: (WhereNode, GroupByNode, OrderByNode, LimitNode, JoinNode),
            WhereNode: (WhereNode, GroupByNode, OrderByNode, LimitNode),
            OrderByNode: (LimitNode,),
            LimitNode: (OffsetNode,),
            GroupByNode: (HavingNode, OrderByNode, LimitNode),
            HavingNode: (OrderByNode, LimitNode),
            UnionNode: (),
            ExceptNode: (),
            IntersectNode: (),
            InsertNode: (ReturningNode,),
        }

    def escape_identifier(self, identifier: str):
        if not self.escape_identifiers_enabled:
            return identifier
        return self._escape_identifier(identifier)

    @abstractmethod
    def _escape_identifier(self, identifier: str): ...

    def normalize_identifiers_arg(self, *args) -> str:
        arg = args[0]
        if isinstance(arg, str):
            arg = [arg]
        identifiers = []

        for identifier in arg:
            identifier = str(identifier).strip()

            if len(parts := ALIAS_SPLIT_REGEX.split(identifier, maxsplit=1)) == 2:
                base, alias = parts[0].strip(), parts[1].strip()

                identifier = self.validate_identifier(base)
                if not ALIAS_REGEX.match(alias):
                    raise ValueError(f"Invalid SQL alias: {alias}")

                identifiers.append(f"{identifier} AS {alias}")
            else:
                identifiers.append(self.validate_identifier(identifier))

        return ",".join(identifiers)

    def validate_identifier(self, identifier: str) -> str:
        if VALID_IDENTIFIER_REGEX.match(identifier):
            identifier = self.escape_identifier(identifier)
        elif (
            AGGREGATE_IDENTIFIER_REGEX.match(identifier)
            or SCALAR_IDENTIFIER_REGEX.match(identifier)
            or EXPRESSION_IDENTIFIER_REGEX.match(identifier)
        ):
            identifier = identifier
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

    def render(self, node: Node) -> str:
        query = ""
        while True:
            query = str(node) + " " + query
            node = node.prev_
            if node is None:
                break
        return query.strip()
