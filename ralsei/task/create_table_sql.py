import sqlalchemy
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table
from dataclasses import field
from typing import Any

from ralsei.connection.utils import executescript
from .base import TaskDef, Task
from .table_output import TableOutput


class CreateTableSql(TaskDef):
    sql: str | list[str]
    table: Table
    view: bool = False
    params: dict[str, Any] = field(default_factory=dict)

    class Impl(Task):
        def __init__(self, this: "CreateTableSql", env: SqlEnvironment) -> None:
            params = {**this.params, "table": this.table, "view": this.view}

            self.__sql = (
                env.render_sql_split(this.sql, **params)
                if isinstance(this.sql, str)
                else [env.render_sql(sql, **params) for sql in this.sql]
            )
            self.output = TableOutput(env, this.table, view=this.view)

        def run(self, conn: sqlalchemy.Connection):
            executescript(conn, self.__sql)
            conn.commit()

        def describe(self) -> str:
            return str(self.__sql[0]) if len(self.__sql) > 0 else ""
