from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ralsei.jinja import SqlEnvironment


class ToSql(ABC):
    """Interface for an object that renders to SQL inside a jinja template"""

    @abstractmethod
    def to_sql(self, env: "SqlEnvironment") -> str:
        """Render to SQL string"""


__all__ = ["ToSql"]
