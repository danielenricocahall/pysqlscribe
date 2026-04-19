from typing import Self

from pysqlscribe.regex_patterns import ALIAS_REGEX

AS = "AS"


class AliasMixin:
    _alias: str | None = None

    def as_(self, alias: str) -> Self:
        if not ALIAS_REGEX.match(alias):
            raise ValueError(f"Invalid SQL alias: {alias}")
        self._alias = alias
        return self

    @property
    def alias(self) -> str:
        if self._alias:
            return f" {AS} {self._alias}"
        return ""
