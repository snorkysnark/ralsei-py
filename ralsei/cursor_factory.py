from typing import Optional, Protocol

import psycopg
from psycopg.rows import dict_row


class CursorFactory(Protocol):
    def create_cursor(self, conn: psycopg.Connection) -> psycopg.Cursor:
        ...


class ClientCursorFactory:
    def create_cursor(self, conn: psycopg.Connection) -> psycopg.Cursor:
        return conn.cursor(row_factory=dict_row)


class ServerCursorFactory:
    def __init__(self, itersize: Optional[int] = None) -> None:
        self.itersize = itersize

    def create_cursor(self, conn: psycopg.Connection) -> psycopg.Cursor:
        cursor = conn.cursor("input_cursor", row_factory=dict_row)

        if self.itersize is not None:
            cursor.itersize = self.itersize

        return cursor
