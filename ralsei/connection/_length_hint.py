from typing import Any, Iterator
import sqlalchemy


class CountableCursorResult:
    def __init__(self, result: sqlalchemy.CursorResult[Any]) -> None:
        self._result = result

    def __iter__(self) -> Iterator[sqlalchemy.Row[Any]]:
        return iter(self._result)

    def __length_hint__(self) -> int:
        return self._result.rowcount
