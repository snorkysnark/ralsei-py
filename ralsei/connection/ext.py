from typing import Any, Iterable, Optional, Self
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams


class ConnectionExt(sqlalchemy.Connection):
    """Extends sqlalchemy's Connection with additional utility methods"""

    def execute_text(
        self, statement: str, parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> sqlalchemy.CursorResult[Any]:
        """Execute a sql string"""
        return self.execute(sqlalchemy.text(statement), parameters)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        "Execute a series of statements, similar to sqlite's executescript"
        for statement in statements:
            self.execute(statement, parameters)

    def executescript_text(
        self,
        statements: Iterable[str],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        "Execute a series of string statements, similar to sqlite's executescript"
        for statement in statements:
            self.execute(sqlalchemy.text(statement), parameters)

    def __enter__(self) -> Self:
        return self


__all__ = ["ConnectionExt"]
