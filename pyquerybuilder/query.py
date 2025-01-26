"""

"""
from abc import abstractmethod, ABC
from typing import Any, Dict, Self, Callable, Sequence

SELECT = "SELECT"
FROM = "FROM"


class QueryBuilder(ABC):
    query: str = ""

    def select(self, *args) -> Self:
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, str):
                fields = arg
            elif isinstance(arg, Sequence):
                fields = ",".join(arg)
        else:
            fields = ",".join(args)
        self.query += f"{SELECT} {fields}"
        return self

    def from_(self, *args) -> Self:
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, str):
                tables = arg
            elif isinstance(arg, Sequence):
                tables = ",".join(args)
        else:
            tables = ",".join(args)
        self.query += f"{FROM} {tables}"


class QueryBuilderRegistry:
    builders: Dict[str, Callable[[], QueryBuilder]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(builder_class: Callable[[], QueryBuilder]) -> Callable[[], QueryBuilder]:
            cls.builders[key] = builder_class()
            return builder_class

        return decorator

    @classmethod
    def get_builder(cls, key: str) -> Callable[[], QueryBuilder]:
        return cls.builders[key]


class Query:


    def __init__(self, dialect: str):
        self._builder = QueryBuilderRegistry.get_builder(dialect)

    def select(self, *args):
        self._builder.select(args)
        return self._builder

    def from_(self, *args):
        self._builder.from_(*args)
        return self._builder


@QueryBuilderRegistry.register("mysql")
class MySQLQueryBuilder(QueryBuilder):
    ...
