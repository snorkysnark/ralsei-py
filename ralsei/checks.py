from typing import Iterable
from typing_extensions import Protocol
from ralsei.context import PsycopgConn
from ralsei.templates import Table


def table_exists(conn: PsycopgConn, table: Table) -> bool:
    return (
        conn.pg()
        .execute(
            """\
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %(name)s AND table_schema = %(schema)s;""",
            {"name": table.name, "schema": table.schema or "public"},
        )
        .fetchone()
        is not None
    )


class ColumnLike(Protocol):
    name: str


def column_exists(conn: PsycopgConn, table: Table, column: str) -> bool:
    return (
        conn.pg()
        .execute(
            """\
            SELECT 1 FROM information_schema.columns
            WHERE table_name = %(name)s AND table_schema = %(schema)s
            AND column_name = %(column)s;""",
            {"name": table.name, "schema": table.schema or "public", "column": column},
        )
        .fetchone()
        is not None
    )


def columns_exist(conn: PsycopgConn, table: Table, columns: Iterable[str]) -> bool:
    for column in columns:
        if not column_exists(conn, table, column):
            return False
    return True


__all__ = ["table_exists", "column_exists", "columns_exist"]
