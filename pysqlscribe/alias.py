from typing import Self


AS = "AS"


class AliasMixin:
    _alias: str | None = None

    def as_(self, alias: str) -> Self:
        self._alias = alias
        return self

    @property
    def alias(self) -> str:
        if self._alias:
            return f" {AS} {self._alias}"
        return ""
