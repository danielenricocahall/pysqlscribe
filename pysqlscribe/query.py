import operator
import os
from abc import abstractmethod, ABC
from copy import copy
from functools import reduce
from typing import Dict, Self, Callable

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.ast.nodes import (
    SelectNode,
    FromNode,
    JoinNode,
    GroupByNode,
    OffsetNode,
    IntersectNode,
    ExceptNode,
    HavingNode,
    OrderByNode,
    WhereNode,
    InsertNode,
    ReturningNode,
    UnionNode,
    LimitNode,
    FetchNextNode,
)
from pysqlscribe.dialects import (
    OracleDialect,
    PostgreSQLDialect,
    Dialect,
    MySQLDialect,
    SQLiteDialect,
)
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


class Query(ABC):
    node: Node | None = None
    __escape_identifiers_enabled: bool = True
    _dialect: Dialect

    @property
    def dialect(self) -> Dialect:
        return self._dialect

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
            ),
            self.dialect,
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
        self.node.add(ReturningNode({"columns": columns}), self.dialect)
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
            ),
            self.dialect,
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
        self.node.add(where_node, self.dialect)
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
            ),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def limit(self, n: int | str):
        self.node.add(LimitNode({"limit": int(n)}), self.dialect)
        self.node = self.node.next_
        return self

    def offset(self, n: int | str):
        self.node.add(OffsetNode({"offset": int(n)}), self.dialect)
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
            ),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def having(self, *args) -> Self:
        having_node = reduce(
            operator.and_, map(lambda arg: HavingNode({"conditions": arg}), args)
        )
        self.node.add(having_node, self.dialect)
        self.node = self.node.next_
        return self

    def union(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(UnionNode({"query": query, "all": all_}), self.dialect)
        self.node = self.node.next_
        return self

    def except_(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(ExceptNode({"query": query, "all": all_}), self.dialect)
        self.node = self.node.next_
        return self

    def intersect(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(IntersectNode({"query": query, "all": all_}), self.dialect)
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
    _dialect: Dialect = MySQLDialect()

    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"


@QueryRegistry.register("oracle")
class OracleQuery(Query):
    _dialect: Dialect = OracleDialect()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def limit(self, n: int | str):
        self.node.add(FetchNextNode({"limit": int(n)}), self.dialect)
        self.node = self.node = self.node.next_
        return self


@QueryRegistry.register("postgres")
class PostgreSQLQuery(Query):
    _dialect: Dialect = PostgreSQLDialect()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'


@QueryRegistry.register("sqlite")
class SQLiteQuery(Query):
    _dialect: Dialect = SQLiteDialect()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
