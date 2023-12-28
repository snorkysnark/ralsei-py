from typing import Iterable
from sqlalchemy import inspect

from ralsei.context import ConnectionContext, Connection
from ralsei.templates import Table, ColumnRendered, SqlalchemyEnvironment


def _get_column_names(conn: Connection, table: Table):
    return set(
        map(
            lambda col: col["name"],
            inspect(conn).get_columns(table.name, table.schema),
        )
    )


def table_exists(ctx: ConnectionContext, table: Table) -> bool:
    return inspect(ctx.connection).has_table(table.name, table.schema)


def columns_exist(ctx: ConnectionContext, table: Table, columns: Iterable[str]) -> bool:
    existing = _get_column_names(ctx.connection, table)

    for column in columns:
        if column not in existing:
            return False
    return True


class add_columns:
    def __init__(
        self,
        env: SqlalchemyEnvironment,
        table: Table,
        columns: Iterable[ColumnRendered],
        if_not_exists: bool = False,
    ) -> None:
        self.statements = [
            env.render(
                """\
            ALTER TABLE {{table}}
            ADD COLUMN {%if if_not_exists%}IF NOT EXISTS {%endif-%}
            {{column.definition}};""",
                table=table,
                column=column,
                if_not_exists=if_not_exists and env.dialect.name != "sqlite",
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_not_exists = if_not_exists

    def __call__(self, ctx: ConnectionContext):
        if self._if_not_exists and ctx.connection.dialect.name == "sqlite":
            existing = _get_column_names(ctx.connection, self._table)
            for column, statement in zip(self._columns, self.statements):
                if not column.name in existing:
                    ctx.connection.execute(statement)
        else:
            ctx.connection.executescript(self.statements)


class drop_columns:
    def __init__(
        self,
        env: SqlalchemyEnvironment,
        table: Table,
        columns: Iterable[ColumnRendered],
        if_exists: bool = False,
    ) -> None:
        self.statements = [
            env.render(
                """\
                ALTER TABLE {{table}}
                DROP COLUMN {%if if_exists%}IF EXISTS {%endif-%}
                {{column.identifier}};""",
                table=table,
                column=column,
                if_exists=if_exists and env.dialect.name != "sqlite",
            )
            for column in columns
        ]
        self._table, self._columns = table, columns
        self._if_exists = if_exists

    def __call__(self, ctx: ConnectionContext):
        if self._if_exists and ctx.connection.dialect.name == "sqlite":
            existing = _get_column_names(ctx.connection, self._table)
            for column, statement in zip(self._columns, self.statements):
                if column.name in existing:
                    ctx.connection.execute(statement)
        else:
            ctx.connection.executescript(self.statements)
