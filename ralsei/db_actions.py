from typing import Iterable
from sqlalchemy import inspect
from sqlalchemy import TextClause

from ralsei.connection import ConnectionEnvironment
from ralsei.types import Table, ColumnRendered
from ralsei.jinja import ISqlEnvironment


def _get_column_names(conn: ConnectionEnvironment, table: Table):
    if table_exists(conn, table):
        return set(
            map(
                lambda col: col["name"],
                inspect(conn.sqlalchemy).get_columns(table.name, table.schema),
            )
        )
    else:
        return set()


def table_exists(conn: ConnectionEnvironment, table: Table) -> bool:
    """Check if table exists"""
    return inspect(conn.sqlalchemy).has_table(table.name, table.schema)


def columns_exist(
    conn: ConnectionEnvironment, table: Table, columns: Iterable[str]
) -> bool:
    """Check if all columns exist on a table"""

    existing = _get_column_names(conn, table)

    for column in columns:
        if column not in existing:
            return False
    return True


class AddColumns:
    """Action for adding columns to a table

    Args:
        env: jinja environment
        table: target table
        columns: columns to add
        if_not_exists: use ``IF NOT EXISTS`` check
    """

    def __init__(
        self,
        env: ISqlEnvironment,
        table: Table,
        columns: Iterable[ColumnRendered],
        if_not_exists: bool = False,
    ) -> None:
        self.statements: list[TextClause] = [
            env.render_sql(
                """\
            ALTER TABLE {{table}}
            ADD COLUMN {%if if_not_exists%}IF NOT EXISTS {%endif-%}
            {{column.definition}};""",
                table=table,
                column=column,
                if_not_exists=if_not_exists
                and env.dialect_info.supports_column_if_not_exists,
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_not_exists = if_not_exists

    def __call__(self, conn: ConnectionEnvironment):
        """Execute action"""
        if self._if_not_exists and not conn.dialect_info.supports_column_if_not_exists:
            existing = _get_column_names(conn, self._table)
            for column, statement in zip(self._columns, self.statements):
                if not column.name in existing:
                    conn.sqlalchemy.execute(statement)
        else:
            conn.sqlalchemy.executescript(self.statements)

    def __str__(self) -> str:
        return "\n".join(map(str, self.statements))


class DropColumns:
    """Action for dropping columns from a table

    Args:
        env: jinja environment
        table: target table
        columns: columns to drop
        if_exists: use ``IF EXISTS`` check
    """

    def __init__(
        self,
        env: ISqlEnvironment,
        table: Table,
        columns: Iterable[ColumnRendered],
        if_exists: bool = False,
    ) -> None:
        self.statements: list[TextClause] = [
            env.render_sql(
                """\
                ALTER TABLE {{table}}
                DROP COLUMN {%if if_exists%}IF EXISTS {%endif-%}
                {{column.identifier}};""",
                table=table,
                column=column,
                if_exists=if_exists and env.dialect_info.supports_column_if_not_exists,
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_exists = if_exists

    def __call__(self, conn: ConnectionEnvironment):
        """Execute action"""
        if self._if_exists and not table_exists(conn, self._table):
            return

        if self._if_exists and not conn.dialect_info.supports_column_if_not_exists:
            existing = _get_column_names(conn, self._table)
            for column, statement in zip(self._columns, self.statements):
                if column.name in existing:
                    conn.sqlalchemy.execute(statement)
        else:
            conn.sqlalchemy.executescript(self.statements)

    def __str__(self) -> str:
        return "\n".join(map(str, self.statements))


__all__ = ["table_exists", "columns_exist", "AddColumns", "DropColumns"]
