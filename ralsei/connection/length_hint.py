from __future__ import annotations
from typing import Any, Generator, Iterable, Iterator, Optional
from sqlalchemy import Connection, Executable, CursorResult, Row
from sqlalchemy.engine.interfaces import _CoreSingleExecuteParams
from contextlib import contextmanager


@contextmanager
def _execute_server_side(
    conn: Connection,
    statement: Executable,
    yield_per: int,
    parameters: Optional[_CoreSingleExecuteParams] = None,
):
    with conn.execution_options(yield_per=yield_per).execute(
        statement, parameters
    ) as result:
        yield result


class _CountableCursorResult:
    def __init__(self, result: CursorResult[Any]) -> None:
        self._result = result

    def __iter__(self) -> Iterator[Row[Any]]:
        return iter(self._result)

    def __length_hint__(self) -> int:
        return self._result.rowcount


@contextmanager
def execute_with_length_hint(
    conn: Connection,
    statement: Executable,
    parameters: Optional[_CoreSingleExecuteParams] = None,
    yield_per: Optional[int] = None,
) -> Generator[Iterable[Row[Any]], None, None]:
    if yield_per is not None:
        with _execute_server_side(
            conn, statement, parameters=parameters, yield_per=yield_per
        ) as result:
            yield result
    elif conn.dialect.name == "sqlite":
        yield conn.execute(statement, parameters).all()
    else:
        yield _CountableCursorResult(conn.execute(statement, parameters))


__all__ = ["execute_with_length_hint"]
