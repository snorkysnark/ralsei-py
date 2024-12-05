from typing import Sequence, Optional, Any
from dataclasses import field

import sqlalchemy

from ralsei.connection import ConnectionEnvironment
from ralsei.graph import Resolves
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect

from .base import TaskDef, Task
from .colum_output import ColumnOutput


class AddColumnsSql(TaskDef):
    sql: str | list[str]
    table: Resolves[Table]
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict[str, Any] = field(default_factory=dict)

    class Impl(Task[ColumnOutput]):
        def __init__(self, this: "AddColumnsSql", env: SqlEnvironment) -> None:
            table = env.resolve(this.table)
            params = {**this.params, "table": table}

            def render_script() -> (
                tuple[list[sqlalchemy.TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(this.sql, str):
                    template_module = env.from_string(this.sql).make_module(params)
                    columns: Optional[Sequence[ColumnBase]] = getattr(
                        template_module, "columns", None
                    )

                    return template_module.render_sql_split(), columns
                else:
                    return [env.render_sql(sql, **params) for sql in this.sql], None

            self.__sql, template_columns = render_script()
            columns = [
                column.render(env, **params)
                for column in expect(
                    this.columns or template_columns,
                    ValueError("Columns not specified"),
                )
            ]

            self.output = ColumnOutput(env, table, columns)

        def run(self, conn: ConnectionEnvironment):
            self.output.add_columns(conn)
            conn.executescript(self.__sql)
            conn.sqlalchemy.commit()
