from typing import Any, Iterable, Optional, Self
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams


class ConnectionExt(sqlalchemy.Connection):
    def execute_text(
        self, statement: str, parameters: Optional[_CoreAnyExecuteParams] = None
    ) -> sqlalchemy.CursorResult[Any]:
        return self.execute(sqlalchemy.text(statement), parameters)

    def executescript_text(
        self,
        statements: Iterable[str],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(sqlalchemy.text(statement), parameters)

    def executescript(
        self,
        statements: Iterable[sqlalchemy.Executable],
        parameters: Optional[_CoreSingleExecuteParams] = None,
    ):
        for statement in statements:
            self.execute(statement, parameters)

    def __enter__(self) -> Self:
        return self


__all__ = ["ConnectionExt"]
