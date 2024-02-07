from typing import Iterable
from sqlalchemy import inspect
from sqlalchemy import TextClause

from ralsei.jinjasql import JinjaSqlConnection
from ralsei.types import Table, ColumnRendered
from ralsei.jinja import SqlEnvironment


def _get_column_names(jsql: JinjaSqlConnection, table: Table):
    if table_exists(jsql, table):
        return set(
            map(
                lambda col: col["name"],
                inspect(jsql.connection).get_columns(table.name, table.schema),
            )
        )
    else:
        return set()


def table_exists(jsql: JinjaSqlConnection, table: Table) -> bool:
    return inspect(jsql.connection).has_table(table.name, table.schema)


def columns_exist(
    jsql: JinjaSqlConnection, table: Table, columns: Iterable[str]
) -> bool:
    existing = _get_column_names(jsql, table)

    for column in columns:
        if column not in existing:
            return False
    return True


class AddColumns:
    def __init__(
        self,
        env: SqlEnvironment,
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
                and env.dialect.supports_column_if_not_exists,
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_not_exists = if_not_exists

    def __call__(self, jsql: JinjaSqlConnection):
        if self._if_not_exists and not jsql.dialect.supports_column_if_not_exists:
            existing = _get_column_names(jsql, self._table)
            for column, statement in zip(self._columns, self.statements):
                if not column.name in existing:
                    jsql.connection.execute(statement)
        else:
            jsql.connection.executescript(self.statements)


class DropColumns:
    def __init__(
        self,
        env: SqlEnvironment,
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
                if_exists=if_exists and env.dialect.supports_column_if_not_exists,
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_exists = if_exists

    def __call__(self, jsql: JinjaSqlConnection):
        if self._if_exists and not jsql.dialect.supports_column_if_not_exists:
            existing = _get_column_names(jsql, self._table)
            for column, statement in zip(self._columns, self.statements):
                if column.name in existing:
                    jsql.connection.execute(statement)
        else:
            jsql.connection.executescript(self.statements)


__all__ = ["table_exists", "columns_exist", "AddColumns", "DropColumns"]
