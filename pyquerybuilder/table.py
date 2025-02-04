from pyquerybuilder.query import Query
from pyquerybuilder.regex_patterns import VALID_IDENTIFIER_REGEX


class InvalidFieldsException(Exception): ...


class InvalidTableNameException(Exception): ...


class Table(Query):
    def __init__(self, name: str, *fields, schema: str | None = None):
        self.name = name
        for field in fields:
            setattr(self, field, None)
        self.schema = schema

    @property
    def name(self):
        if self.schema:
            return f"{self.schema}.{self._name}"
        return self._name

    @name.setter
    def name(self, table_name: str):
        if not VALID_IDENTIFIER_REGEX.match(table_name):
            raise InvalidTableNameException(f"Invalid table name {table_name}")
        self._name = table_name

    def select(self, *fields):
        try:
            assert all(hasattr(self, field) for field in fields)
            return super().select(*fields).from_(self.name)
        except AssertionError:
            raise InvalidFieldsException(
                f"Table {self.name} doesn't have one or more of the fields provided"
            )
