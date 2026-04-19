from typing import runtime_checkable, Protocol, Any, Self


@runtime_checkable
class DialectProtocol(Protocol):
    def validate(self, current_node, next_node) -> None: ...

    def escape_identifier(self, identifier: str) -> str: ...
    def validate_identifier(self, identifier: str) -> str: ...
    def normalize_identifiers_args(self, args: Any) -> str: ...
    def escape_value(self, value) -> str: ...


@runtime_checkable
class Subqueryish(Protocol):
    @property
    def select(self) -> Self: ...


@runtime_checkable
class TableProtocol(Protocol):
    @property
    def columns(self): ...


@runtime_checkable
class ColumnProtocol(Protocol):
    @property
    def fully_qualified_name(self) -> str: ...

    @property
    def alias(self) -> str: ...


@runtime_checkable
class CaseProtocol(Protocol):
    def else_(self): ...

    def when(self): ...

    @property
    def expression(self) -> str: ...
