from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


class ToSql(ABC):
    @abstractmethod
    def to_sql(self, env: "ISqlEnvironment") -> str: ...


__all__ = ["ToSql"]
