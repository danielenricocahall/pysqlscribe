import operator
import os
from abc import abstractmethod, ABC
from copy import copy
from enum import Enum
from functools import reduce
from typing import Any, Dict, Self, Callable, Tuple

from pysqlscribe.env_utils import str2bool
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
    WILDCARD_REGEX,
    ALIAS_SPLIT_REGEX,
    ALIAS_REGEX,
    SCALAR_IDENTIFIER_REGEX,
    EXPRESSION_IDENTIFIER_REGEX,
)

SELECT = "SELECT"
FROM = "FROM"
WHERE = "WHERE"
LIMIT = "LIMIT"
JOIN = "JOIN"
ORDER_BY = "ORDER BY"
AND = "AND"
FETCH_NEXT = "FETCH NEXT"
OFFSET = "OFFSET"
GROUP_BY = "GROUP BY"
HAVING = "HAVING"
ALL = "ALL"
UNION = "UNION"
UNION_ALL = f"UNION {ALL}"
EXCEPT = "EXCEPT"
EXCEPT_ALL = f"EXCEPT {ALL}"
INTERSECT = "INTERSECT"
INTERSECT_ALL = f"INTERSECT {ALL}"
INSERT = "INSERT"
INTO = "INTO"
VALUES = "VALUES"


class JoinType(str, Enum):
    INNER: str = "INNER"
    OUTER: str = "OUTER"
    LEFT: str = "LEFT"
    RIGHT: str = "RIGHT"
    CROSS: str = "CROSS"
    NATURAL: str = "NATURAL"

    def __str__(self):
        return self.value


def reconcile_args_into_string(*args, escape_identifier: Callable[[str], str]) -> str:
    arg = args[0]
    if isinstance(arg, str):
        arg = [arg]
    identifiers = []

    for identifier in arg:
        identifier = str(identifier).strip()

        if len(parts := ALIAS_SPLIT_REGEX.split(identifier, maxsplit=1)) == 2:
            base, alias = parts[0].strip(), parts[1].strip()

            identifier = validate_identifier(base, escape_identifier)
            if not ALIAS_REGEX.match(alias):
                raise ValueError(f"Invalid SQL alias: {alias}")

            identifiers.append(f"{identifier} AS {alias}")
        else:
            identifiers.append(validate_identifier(identifier, escape_identifier))

    return ",".join(identifiers)


def validate_identifier(identifier: str, escape_identifier) -> str:
    if VALID_IDENTIFIER_REGEX.match(identifier):
        identifier = escape_identifier(identifier)
    elif (
        AGGREGATE_IDENTIFIER_REGEX.match(identifier)
        or SCALAR_IDENTIFIER_REGEX.match(identifier)
        or EXPRESSION_IDENTIFIER_REGEX.match(identifier)
    ):
        identifier = identifier
    else:
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


class InvalidNodeException(Exception): ...


class Node(ABC):
    next_: Self | None = None
    prev_: Self | None = None
    state: dict[str, Any]

    def __init__(self, state):
        self.state = state

    def add(self, next_: "Node"):
        if not isinstance(next_, self.valid_next_nodes):
            raise InvalidNodeException()
        next_.prev_ = self
        self.next_ = next_

    @property
    @abstractmethod
    def valid_next_nodes(self) -> Tuple[type[Self], ...]: ...


class SelectNode(Node):
    @property
    def valid_next_nodes(self):
        return (FromNode,)

    def __str__(self):
        return f"{SELECT} {self.state['columns']}"


class FromNode(Node):
    @property
    def valid_next_nodes(self):
        return (
            JoinNode,
            WhereNode,
            GroupByNode,
            OrderByNode,
            LimitNode,
            UnionNode,
            ExceptNode,
            IntersectNode,
        )

    def __str__(self):
        return f"{FROM} {self.state['tables']}"


class JoinNode(Node):
    def __init__(self, state):
        super().__init__(state)
        self.join_type = state.get("join_type", JoinType.INNER)
        self.table = state["table"]
        if (condition := state.get("condition")) and self.join_type in (
            JoinType.NATURAL,
            JoinType.CROSS,
        ):
            raise InvalidJoinException(
                "Conditions need to be supplied for any join which is not NATURAL or CROSS"
            )
        self.condition = condition

    @property
    def valid_next_nodes(self):
        return WhereNode, GroupByNode, OrderByNode, LimitNode, JoinNode

    def __str__(self):
        return f"{self.join_type} {JOIN} {self.table} " + (
            f"ON {self.condition}"
            if self.join_type not in (JoinType.NATURAL, JoinType.CROSS)
            else ""
        )


class WhereNode(Node):
    @property
    def valid_next_nodes(self):
        return GroupByNode, OrderByNode, LimitNode, WhereNode

    def __str__(self):
        return f"{WHERE} {self.state['conditions']}"

    def __and__(self, other):
        if isinstance(other, WhereNode):
            compound_condition = (
                f"{self.state['conditions']} {AND} {other.state['conditions']}"
            )
            return WhereNode({"conditions": compound_condition})


class OrderByNode(Node):
    @property
    def valid_next_nodes(self):
        return LimitNode

    def __str__(self):
        return f"{ORDER_BY} {self.state['columns']}"


class LimitNode(Node):
    @property
    def valid_next_nodes(self):
        return OffsetNode

    def __str__(self):
        return f"{LIMIT} {self.state['limit']}"


class FetchNextNode(LimitNode):
    @property
    def valid_next_nodes(self):
        return ()

    def __str__(self):
        return f"{FETCH_NEXT} {self.state['limit']} ROWS ONLY"


class OffsetNode(Node):
    @property
    def valid_next_nodes(self):
        return ()

    def __str__(self):
        return f"{OFFSET} {self.state['offset']}"


class GroupByNode(Node):
    @property
    def valid_next_nodes(self):
        return HavingNode, OrderByNode, LimitNode

    def __str__(self):
        return f"{GROUP_BY} {self.state['columns']}"


class HavingNode(Node):
    @property
    def valid_next_nodes(self):
        return OrderByNode, LimitNode

    def __str__(self):
        return f"{HAVING} {self.state['conditions']}"

    def __and__(self, other):
        if isinstance(other, HavingNode):
            compound_condition = (
                f"{self.state['conditions']} {AND} {other.state['conditions']}"
            )
            return HavingNode({"conditions": compound_condition})


class CombineNode(Node, ABC):
    def __init__(self, state):
        super().__init__(state)
        self.query = state["query"]
        self.all = state.get("all", False)

    @property
    @abstractmethod
    def operation(self): ...

    @property
    def valid_next_nodes(self):
        return ()

    def __str__(self):
        if isinstance(self.query, Query):
            return f"{self.operation} {self.query.build(clear=False)}"
        return f"{self.operation} {self.query}"


class UnionNode(CombineNode):
    @property
    def operation(self):
        return UNION if not self.all else UNION_ALL


class ExceptNode(CombineNode):
    @property
    def operation(self):
        return EXCEPT if not self.all else EXCEPT_ALL


class IntersectNode(CombineNode):
    @property
    def operation(self):
        return INTERSECT if not self.all else INTERSECT_ALL


class InsertNode(Node):
    @property
    def valid_next_nodes(self):
        return (ReturningNode,)

    def __str__(self):
        if isinstance(self.state["values"], str):
            values = f"({self.state['values']})"
        elif isinstance(self.state["values"], list):
            values = ",".join([f"({v})" for v in self.state["values"]])
        else:
            raise ValueError(f"Invalid values: {self.state['values']}")
        columns = f" ({self.state['columns']})" if self.state["columns"] else ""
        return f"{INSERT} {INTO} {self.state['table']}{columns} {VALUES} {values}"


class ReturningNode(Node):
    @property
    def valid_next_nodes(self):
        return ()

    def __str__(self):
        return f"RETURNING {self.state['columns']}"


class InvalidJoinException(Exception): ...


class Query(ABC):
    node: Node | None = None
    __escape_identifiers_enabled: bool = True

    def select(self, *args) -> Self:
        columns = self._resolve_columns(*args)
        if not self.node:
            self.node = SelectNode({"columns": columns})
        return self

    def from_(self, *args) -> Self:
        self.node.add(
            FromNode(
                {
                    "tables": reconcile_args_into_string(
                        args, escape_identifier=self.escape_identifier
                    )
                }
            )
        )
        self.node = self.node.next_
        return self

    def insert(self, *columns, **kwargs) -> Self:
        table = kwargs.get("into")
        values = kwargs.get("values")
        if table is None or values is None:
            raise ValueError("Insert queries require `into` and `values` keywords.")
        values = self._resolve_insert_values(columns, values)
        columns = reconcile_args_into_string(
            columns, escape_identifier=self.escape_identifier
        )
        table = reconcile_args_into_string(
            table, escape_identifier=self.escape_identifier
        )
        if not self.node:
            self.node = InsertNode(
                {"columns": columns, "table": table, "values": values}
            )
        return self

    def returning(self, *args) -> Self:
        columns = self._resolve_columns(*args)
        self.node.add(ReturningNode({"columns": columns}))
        self.node = self.node.next_
        return self

    @staticmethod
    def _resolve_insert_values(columns, values) -> list[str]:
        if isinstance(values, tuple):
            values = [values]
        assert all(
            (len(columns) == 0 or len(columns) == len(value) for value in values)
        ), "Number of columns and values must match"
        values = [f"{','.join(map(str, value))}" for value in values]
        return values

    def _resolve_columns(self, *args) -> str:
        if not args:
            args = ["*"]
        if WILDCARD_REGEX.match(args[0]):
            columns = args[0]
        else:
            columns = reconcile_args_into_string(
                args, escape_identifier=self.escape_identifier
            )
        return columns

    def join(
        self, table: str, join_type: str = JoinType.INNER, condition: str | None = None
    ) -> Self:
        self.node.add(
            JoinNode(
                {
                    "join_type": join_type.upper(),
                    "table": reconcile_args_into_string(
                        table, escape_identifier=self.escape_identifier
                    ),
                    "condition": condition,
                }
            )
        )
        self.node = self.node.next_
        return self

    def inner_join(self, table: str, condition: str):
        return self.join(table, JoinType.INNER, condition)

    def outer_join(self, table: str, condition: str):
        return self.join(table, JoinType.OUTER, condition)

    def left_join(self, table: str, condition: str):
        return self.join(table, JoinType.LEFT, condition)

    def right_join(self, table: str, condition: str):
        return self.join(table, JoinType.RIGHT, condition)

    def cross_join(self, table: str):
        return self.join(table, JoinType.CROSS)

    def natural_join(self, table: str):
        return self.join(table, JoinType.NATURAL)

    def where(self, *args) -> Self:
        where_node = reduce(
            operator.and_, map(lambda arg: WhereNode({"conditions": arg}), args)
        )
        self.node.add(where_node)
        self.node = self.node.next_
        return self

    def order_by(self, *args) -> Self:
        self.node.add(
            OrderByNode(
                {
                    "columns": reconcile_args_into_string(
                        args, escape_identifier=self.escape_identifier
                    )
                }
            )
        )
        self.node = self.node.next_
        return self

    def limit(self, n: int | str):
        self.node.add(LimitNode({"limit": int(n)}))
        self.node = self.node.next_
        return self

    def offset(self, n: int | str):
        self.node.add(OffsetNode({"offset": int(n)}))
        self.node = self.node.next_
        return self

    def group_by(self, *args) -> Self:
        self.node.add(
            GroupByNode(
                {
                    "columns": reconcile_args_into_string(
                        args, escape_identifier=self.escape_identifier
                    )
                }
            )
        )
        self.node = self.node.next_
        return self

    def having(self, *args) -> Self:
        having_node = reduce(
            operator.and_, map(lambda arg: HavingNode({"conditions": arg}), args)
        )
        self.node.add(having_node)
        self.node = self.node.next_
        return self

    def union(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(UnionNode({"query": query, "all": all_}))
        self.node = self.node.next_
        return self

    def except_(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(ExceptNode({"query": query, "all": all_}))
        self.node = self.node.next_
        return self

    def intersect(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(IntersectNode({"query": query, "all": all_}))
        self.node = self.node.next_
        return self

    def build(self, clear: bool = True) -> str:
        node = self.node
        query = ""
        while True:
            query = str(node) + " " + query
            node = node.prev_
            if node is None:
                break
        if clear:
            # we provide an option to not clear the builder in the event the developer needs
            # to debug or needs to reuse the value. By default, we do immediately after building the query
            self.node = None
        return query.strip()

    def __str__(self):
        return self.build(clear=False)

    def disable_escape_identifiers(self):
        self.__escape_identifiers_enabled = False
        return self

    def enable_escape_identifiers(self):
        self.__escape_identifiers_enabled = True
        return self

    @property
    def escape_identifiers_enabled(self):
        if not str2bool(os.environ.get("PYSQLSCRIBE_ESCAPE_IDENTIFIERS", "true")):
            return False
        return self.__escape_identifiers_enabled

    @abstractmethod
    def _escape_identifier(self, identifier: str): ...

    def escape_identifier(self, identifier: str):
        if not self.escape_identifiers_enabled:
            return identifier
        return self._escape_identifier(identifier)


class QueryRegistry:
    builders: Dict[str, Query] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(builder_class: Callable[[], Query]) -> Callable[[], Query]:
            cls.builders[key] = builder_class()
            return builder_class

        return decorator

    @classmethod
    def get_builder(cls, key: str) -> Query:
        return copy(cls.builders[key])


@QueryRegistry.register("mysql")
class MySQLQuery(Query):
    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"


@QueryRegistry.register("oracle")
class OracleQuery(Query):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def limit(self, n: int | str):
        self.node.add(FetchNextNode({"limit": int(n)}))
        self.node = self.node = self.node.next_
        return self


@QueryRegistry.register("postgres")
class PostgreSQLQuery(Query):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'


@QueryRegistry.register("sqlite")
class SQLiteQuery(Query):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
