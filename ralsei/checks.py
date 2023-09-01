"""
Checks for table/column's existence

You can use these to implement [ralsei.task.Task.exists][]
"""

from typing import Iterable
from ralsei.connection import PsycopgConn
from ralsei.templates import Table


def table_exists(conn: PsycopgConn, table: Table) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema

    Returns:
        True if table exists, False otherwise
    """
    return (
        conn.pg.execute(
            """\
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %(name)s AND table_schema = %(schema)s;""",
            {"name": table.name, "schema": table.schema or "public"},
        ).fetchone()
        is not None
    )


def column_exists(conn: PsycopgConn, table: Table, column: str) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema
        column: column name

    Returns:
        True if column exists, False otherwise
    """
    return (
        conn.pg.execute(
            """\
            SELECT 1 FROM information_schema.columns
            WHERE table_name = %(name)s AND table_schema = %(schema)s
            AND column_name = %(column)s;""",
            {"name": table.name, "schema": table.schema or "public", "column": column},
        ).fetchone()
        is not None
    )


def columns_exist(conn: PsycopgConn, table: Table, columns: Iterable[str]) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema
        columns: column names

    Returns:
        True if all column exist, False otherwise
    """
    for column in columns:
        if not column_exists(conn, table, column):
            return False
    return True


__all__ = ["table_exists", "column_exists", "columns_exist"]
