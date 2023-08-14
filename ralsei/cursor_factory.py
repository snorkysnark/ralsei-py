from typing import Optional, Protocol

import psycopg
from psycopg.rows import dict_row


class CursorFactory(Protocol):
    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        ...


class ClientCursorFactory:
    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        return conn.cursor(row_factory=dict_row)


class ServerCursorFactory:
    def __init__(
        self, name: str = "input_cursor", itersize: Optional[int] = None
    ) -> None:
        self.name = name
        self.itersize = itersize

    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        cursor = conn.cursor(self.name, row_factory=dict_row, withhold=withhold)

        if self.itersize is not None:
            cursor.itersize = self.itersize

        return cursor
