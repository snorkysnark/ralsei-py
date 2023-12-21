from typing import Callable, Iterable
from sqlalchemy import inspect

from ralsei.context import Context, Connection
from ralsei.templates import Table, ColumnRendered, SqlalchemyEnvironment


def _get_column_names(conn: Connection, table: Table):
    return set(
        map(
            lambda col: col["name"],
            inspect(conn).get_columns(table.name, table.schema),
        )
    )


def table_exists(ctx: Context, table: Table) -> bool:
    return inspect(ctx.connection).has_table(table.name, table.schema)


def columns_exist(ctx: Context, table: Table, columns: Iterable[str]) -> bool:
    existing = _get_column_names(ctx.connection, table)

    for column in columns:
        if column not in existing:
            return False
    return True


def add_columns(
    env: SqlalchemyEnvironment,
    table: Table,
    columns: Iterable[ColumnRendered],
    if_not_exists: bool = False,
) -> Callable[[Context], None]:
    statements = [
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

    if if_not_exists and env.dialect.name == "sqlite":

        def run_manual_if_not_exists(ctx: Context):
            existing = _get_column_names(ctx.connection, table)
            for column, statement in zip(columns, statements):
                if not column.name in existing:
                    ctx.connection.execute(statement)

        return run_manual_if_not_exists
    else:

        def run(ctx: Context):
            ctx.connection.executescript(statements)

        return run


def drop_columns(
    env: SqlalchemyEnvironment,
    table: Table,
    columns: Iterable[ColumnRendered],
    if_exists: bool = False,
) -> Callable[[Context], None]:
    statements = [
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

    if if_exists and env.dialect.name == "sqlite":

        def run_manual_if_exists(ctx: Context):
            existing = _get_column_names(ctx.connection, table)
            for column, statement in zip(columns, statements):
                if column.name in existing:
                    ctx.connection.execute(statement)

        return run_manual_if_exists
    else:

        def run(ctx: Context):
            ctx.connection.executescript(statements)

        return run
