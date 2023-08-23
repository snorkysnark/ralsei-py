from abc import ABC, abstractmethod
from typing import Optional

import psycopg
from psycopg.rows import dict_row


class CursorFactory(ABC):
    """"""

    @abstractmethod
    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        """
        Creates a [(client- or server-side)](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#cursor-types)
        psycopg.Cursor

        Args:
        - conn (`psycopg.Connection`): connection
        - [withhold](https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.ServerCursor.withhold)
          (bool): If the cursor can be used after the creating transaction has committed.

        The returned cursor should use row_factory=dict_row by default
        """
        ...


class ClientCursorFactory(CursorFactory):
    """
    The default cursor factory, creates an in-memory cursor
    """

    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        return conn.cursor(row_factory=dict_row)


class ServerCursorFactory:
    def __init__(
        self, name: str = "input_cursor", itersize: Optional[int] = None
    ) -> None:
        """
        Use this if the number of rows is too large to fit into memory

        Args:
        - name (str), optional: how to name this cursor on the backend
        - itersize (int), optional: how many rows to load into memory at once
        """

        self._name = name
        self._itersize = itersize

    def create_cursor(self, conn: psycopg.Connection, withhold: bool) -> psycopg.Cursor:
        cursor = conn.cursor(self._name, row_factory=dict_row, withhold=withhold)

        if self._itersize is not None:
            cursor.itersize = self._itersize

        return cursor
