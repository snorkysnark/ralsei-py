"""
Checks for table/column's existence

You can use these to implement [ralsei.task.Task.exists][]
"""

from typing import Iterable

from ralsei.templates import Table
from ralsei.context import Context


def table_exists(ctx: Context, table: Table, view: bool = False) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema
        view: whether this is a VIEW instead of a TABLE

    Returns:
        True if table exists, False otherwise
    """
    return (
        ctx.render_execute(
            """\
            SELECT 1 FROM information_schema.{{ ('views' if view else 'tables') | identifier }}
            WHERE table_name = :name AND table_schema = :schema;
            """,
            {"view": view},
            {"name": table.name, "schema": table.schema or "public"},
        ).fetchone()
        is not None
    )


def column_exists(ctx: Context, table: Table, column: str) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema
        column: column name

    Returns:
        True if column exists, False otherwise
    """
    return (
        ctx.connection.execute(
            (
                """\
             SELECT 1 FROM information_schema.columns
             WHERE table_name = :name AND table_schema = :schema
             AND column_name = :column;"""
            ),
            {
                "name": table.name,
                "schema": table.schema or "public",
                "column": column,
            },
        ).fetchone()
        is not None
    )


def columns_exist(ctx: Context, table: exp.Table, columns: Iterable[str]) -> bool:
    """
    Args:
        conn: db connection
        table: table name and schema
        columns: column names

    Returns:
        True if all column exist, False otherwise
    """
    for column in columns:
        if not column_exists(ctx, table, column):
            return False
    return True


__all__ = ["table_exists", "column_exists", "columns_exist"]
