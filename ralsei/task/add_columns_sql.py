from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from .common import (
    TaskDef,
    TaskImpl,
    Table,
    Renderable,
    Column,
    Context,
    merge_params,
    checks,
)


@dataclass
class AddColumnsSql(TaskDef):
    sql: str
    table: Table
    columns: Optional[list[Renderable[Column]]] = None
    params: dict = {}

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, ctx: Context) -> None:
            jinja_args = merge_params({"table": this.table}, this.params)

            template = ctx.jinja.from_string_script(this.sql)
            script_module = template.module

            columns = this.columns
            if columns is None:
                # Get columns variable from template: {% set columns = [...] %}
                columns = getattr(script_module, "columns", None)
                if columns is None:
                    raise ValueError("Columns not specified")

            rendered_columns = list(
                map(
                    lambda col: col.render(ctx.jinja.inner, **jinja_args),
                    columns,
                )
            )
            self.__column_names = list(map(lambda col: col.name, rendered_columns))

            self.__add_columns = ctx.jinja.render(
                """\
                {% set sep = joiner(',\n') -%}

                ALTER TABLE {{ table }}
                {% for column in columns -%}
                {{ sep() }}ADD COLUMN {{ column.definition }}
                {%- endfor %};""",
                merge_params(jinja_args, {"columns": rendered_columns}),
            )
            self.__sql = template.render(**jinja_args)
            self.__drop_columns = ctx.jinja.render(
                """\
                {% set sep = joiner(',\n') -%}

                ALTER TABLE {{ table }}
                {% for column in columns -%}
                {{ sep() }}DROP COLUMN {{ column.identifier }}
                {%- endfor %};""",
                merge_params(jinja_args, {"columns": rendered_columns}),
            )

            self.__table = this.table

        def exists(self, ctx: Context) -> bool:
            return checks.columns_exist(ctx, self.__table, self.__column_names)

        def run(self, ctx: Context) -> None:
            ctx.connection.execute(self.__add_columns)
            ctx.connection.executescript(self.__sql)

        def delete(self, ctx: Context) -> None:
            ctx.connection.execute(self.__drop_columns)
