from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence, cast
from sqlalchemy import TextClause

from .base import TaskImpl, TaskDef
from ralsei.graph import OutputOf
from ralsei.types import Table, ColumnBase
from ralsei.jinja import SqlalchemyEnvironment
from ralsei.utils import expect_optional
from ralsei import db_actions
from ralsei.context import ConnectionContext


@dataclass
class AddColumnsSql(TaskDef):
    """Adds the specified Columns to an existing Table
    and runs the SQL script to fill them with data

    Variables passed to the template: :py:attr:`~table`, `**`:py:attr:`~params` |br|
    Columns can be defined in the template itself, using ``{% set columns = [...] %}``

    Example:

        **postprocess.sql**

        .. code-block:: sql

            {% set columns = [Column("name_upper", "TEXT")] -%}

            UPDATE {{table}}
            SET name_upper = UPPER(name);


        **pipeline.py**

        .. code-block:: python

            "postprocess": AddColumnsSql(
                sql=Path("./postprocess.sql").read_text(),
                table=TABLE_people,
            )
    """

    sql: str | list[str]
    """Sql template strings

    Individual statements must be either separated by ``{%split%}`` tag or pre-split into a list
    """
    table: Table | OutputOf
    """Table to add columns to

    May be the output of another task
    """
    columns: Optional[Sequence[ColumnBase]] = None
    """these column definitions take precedence over those defined in the template"""
    params: dict = field(default_factory=dict)
    """parameters passed to the jinja template"""

    class Impl(TaskImpl):
        def __init__(self, this: AddColumnsSql, env: SqlalchemyEnvironment) -> None:
            self._table = env.resolve(this.table)

            def render_script() -> (
                tuple[list[TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(this.sql, str):
                    template_module = env.from_string(this.sql).make_module(
                        {"table": self._table, **this.params}
                    )
                    columns = cast(
                        Optional[Sequence[ColumnBase]],
                        getattr(template_module, "columns", None),
                    )

                    return template_module.render_split(), columns
                else:
                    return [
                        env.render(sql, table=self._table, **this.params)
                        for sql in this.sql
                    ], None

            self._sql, template_columns = render_script()
            columns = expect_optional(
                this.columns or template_columns, ValueError("Columns not specified")
            )

            rendered_columns = [
                col.render(env.text, table=self._table, **this.params)
                for col in columns
            ]
            self._column_names = [col.name for col in rendered_columns]

            self._add_columns = db_actions.AddColumns(
                env, self._table, rendered_columns
            )
            self._drop_columns = db_actions.DropColumns(
                env, self._table, rendered_columns
            )

        @property
        def output(self) -> Any:
            return self._table

        def exists(self, ctx: ConnectionContext) -> bool:
            return db_actions.columns_exist(ctx, self._table, self._column_names)

        def run(self, ctx: ConnectionContext) -> None:
            self._add_columns(ctx)
            ctx.connection.executescript(self._sql)

        def delete(self, ctx: ConnectionContext) -> None:
            self._drop_columns(ctx)

        def sql_scripts(self) -> Iterable[tuple[str, object | list[object]]]:
            yield "Add columns", self._add_columns.statements
            yield "Main", self._sql
            yield "Drop columns", self._drop_columns.statements


__all__ = ["AddColumnsSql"]
