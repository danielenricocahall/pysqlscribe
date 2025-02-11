import operator
from abc import abstractmethod, ABC
from enum import Enum
from functools import reduce
from typing import Any, Dict, Self, Callable, Tuple

from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
    WILDCARD_REGEX,
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
        identifier = identifier.strip()
        if VALID_IDENTIFIER_REGEX.match(identifier):
            identifiers.append(escape_identifier(identifier))
        elif AGGREGATE_IDENTIFIER_REGEX.match(identifier):
            identifiers.append(identifier)
        else:
            raise ValueError(f"Invalid SQL identifier: {identifier}")
    return ",".join(identifiers)


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
        return JoinNode, WhereNode, GroupByNode, OrderByNode, LimitNode

    def __str__(self):
        return f"{FROM} {self.state['tables']}"


class JoinNode(Node):
    def __init__(self, state):
        super().__init__(state)
        self.join_type = state.get("join_type", JoinType.INNER)
        self.table = state["table"]
        self.condition = state.get("condition")

    @property
    def valid_next_nodes(self):
        return WhereNode, GroupByNode, OrderByNode, LimitNode, JoinNode

    def __str__(self):
        return f"{self.join_type} {JOIN} {self.table} " + (
            f"ON {self.condition}" if self.join_type != JoinType.NATURAL else ""
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


class InvalidJoinException(Exception): ...


class Query(ABC):
    node: Node | None = None

    def select(self, *args) -> Self:
        if WILDCARD_REGEX.match(args[0]):
            columns = args[0]
        else:
            columns = reconcile_args_into_string(
                args, escape_identifier=self.escape_identifier
            )
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

    def join(
        self, table: str, join_type: str = JoinType.INNER, condition: str | None = None
    ) -> Self:
        if condition and join_type in (JoinType.NATURAL, JoinType.CROSS):
            raise InvalidJoinException(
                "Conditions need to be supplied for any join which is not NATURAL or CROSS"
            )

        self.node.add(
            JoinNode(
                {
                    "join_type": join_type.upper(),
                    "table": self.escape_identifier(table),
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
        self.node = self.node = self.node.next_
        return self

    def offset(self, n: int | str):
        self.node.add(OffsetNode({"offset": int(n)}))
        self.node = self.node = self.node.next_
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

    @abstractmethod
    def escape_identifier(self, identifier: str): ...


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
        return cls.builders[key]


@QueryRegistry.register("mysql")
class MySQLQuery(Query):
    def escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"


@QueryRegistry.register("oracle")
class OracleQuery(Query):
    def escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def limit(self, n: int | str):
        self.node.add(FetchNextNode({"limit": int(n)}))
        self.node = self.node = self.node.next_
        return self


@QueryRegistry.register("postgres")
class PostgreSQLQuery(Query):
    def escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'


@QueryRegistry.register("sqlite")
class SQLiteQuery(Query):
    def escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
