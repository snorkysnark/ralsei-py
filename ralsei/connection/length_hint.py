from __future__ import annotations
from typing import Any, Iterable, Iterator, Optional
from sqlalchemy import Connection, Executable, CursorResult, Row
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams


class _CountableCursorResult:
    def __init__(self, result: CursorResult[Any]) -> None:
        self._result = result

    def __iter__(self) -> Iterator[Row[Any]]:
        return iter(self._result)

    def __length_hint__(self) -> int:
        return self._result.rowcount


def execute_with_length_hint(
    conn: Connection,
    statement: Executable,
    parameters: Optional[_CoreSingleExecuteParams] = None,
) -> Iterable[Row[Any]]:
    if conn.dialect.name == "sqlite":
        return conn.execute(statement, parameters).all()
    else:
        return _CountableCursorResult(conn.execute(statement, parameters))


__all__ = ["execute_with_length_hint"]
