from __future__ import annotations
from typing import Any
from attrs import define, field
import sqlalchemy

from ralsei.injector import DIContext
from ralsei.types import Table
from ralsei.jinja import SqlEnvironment
from ralsei.connection.utils import executescript
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Settled, Task
from .create_table import CreateTableBase


@define(eq=False)
class CreateTableSql(Task):
    sql: str | list[str]
    table: Table
    view: bool = False
    params: dict[str, Any] = field(factory=dict)


@CreateTableSql.impl
class ImplCreateTableSql(CreateTableBase[CreateTableSql]):
    def __init__(self, task: Settled[CreateTableSql], context: DIContext) -> None:
        env = context.get(SqlEnvironment)
        super().__init__(task, env, task.decl.table, task.decl.view)

        params = {**task.decl.params, "table": task.decl.table, "view": task.decl.view}
        self.__sql = (
            env.render_sql_split(task.decl.sql, **params)
            if isinstance(task.decl.sql, str)
            else [env.render_sql(sql, **params) for sql in task.decl.sql]
        )

    def run(self, context: DIContext):
        conn = context.get(sqlalchemy.Connection)
        executescript(conn, self.__sql)
        conn.commit()

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(
            g, self.task.path, str(self.__sql[0]) if len(self.__sql) > 0 else ""
        )
