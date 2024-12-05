from typing import Any, Iterator, Optional, Iterable
import sqlalchemy
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams, _CoreAnyExecuteParams

from ralsei.dialect import DialectMetadata


def execute_text(
    conn: sqlalchemy.Connection,
    statement: str,
    parameters: Optional[_CoreAnyExecuteParams] = None,
) -> sqlalchemy.CursorResult[Any]:
    """Execute a sql string"""
    return conn.execute(sqlalchemy.text(statement), parameters)


def executescript(
    conn: sqlalchemy.Connection,
    statements: Iterable[sqlalchemy.Executable],
    parameters: Optional[_CoreSingleExecuteParams] = None,
):
    "Execute a series of statements, similar to sqlite's executescript"
    for statement in statements:
        conn.execute(statement, parameters)


def executescript_text(
    conn: sqlalchemy.Connection,
    statements: Iterable[str],
    parameters: Optional[_CoreSingleExecuteParams] = None,
):
    "Execute a series of string statements, similar to sqlite's executescript"
    for statement in statements:
        conn.execute(sqlalchemy.text(statement), parameters)


class CountableCursorResult:
    def __init__(self, result: sqlalchemy.CursorResult[Any]) -> None:
        self._result = result

    def __iter__(self) -> Iterator[sqlalchemy.Row[Any]]:
        return iter(self._result)

    def __length_hint__(self) -> int:
        return self._result.rowcount


def execute_with_length_hint(
    conn: sqlalchemy.Connection,
    dialect: DialectMetadata,
    statement: sqlalchemy.Executable,
    parameters: Optional[_CoreSingleExecuteParams] = None,
) -> Iterable[sqlalchemy.Row[Any]]:
    """Execute a sql expression, returning an object with a :py:meth:`object.__length_hint__` method,
    letting you see the estimated number or rows.

    Concrete implementation depends on the sql dialect
    """

    result = conn.execute(statement, parameters)
    return CountableCursorResult(result) if dialect.supports_rowcount else result.all()
