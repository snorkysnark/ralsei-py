from __future__ import annotations
from typing import Any
from attrs import define, field
import sqlalchemy

from ralsei.injector import DIContext
from ralsei.types import Table
from ralsei.jinja import SqlEnvironment
from ralsei.connection.utils import executescript
from ralsei import db_actions
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Impl, Task


@define(eq=False)
class CreateTableSql(Task["ImplCreateTableSql"]):
    sql: str | list[str]
    table: Table
    view: bool = False
    params: dict[str, Any] = field(factory=dict)

    impl: ImplCreateTableSql = field(init=False, repr=False)


class ImplCreateTableSql(Impl[CreateTableSql]):
    def __init__(self, decl: CreateTableSql, env: SqlEnvironment) -> None:
        params = {**decl.params, "table": decl.table, "view": decl.view}

        self.sql = (
            env.render_sql_split(decl.sql, **params)
            if isinstance(decl.sql, str)
            else [env.render_sql(sql, **params) for sql in decl.sql]
        )
        self.drop_sql = env.render_sql(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            table=decl.table,
            view=decl.view,
        )

    def run(self, decl: CreateTableSql, context: DIContext):
        conn = context.get(sqlalchemy.Connection)

        executescript(conn, self.sql)
        conn.commit()

    def delete(self, decl: CreateTableSql, context: DIContext):
        conn = context.get(sqlalchemy.Connection)

        conn.execute(self.drop_sql)
        conn.commit()

    def skip(self, decl: CreateTableSql, context: DIContext) -> bool:
        return db_actions.table_exists(context.get(sqlalchemy.Connection), decl.table)

    def visualize(self, decl: CreateTableSql, g: VisualGraph) -> VisualNode:
        return WindowNode(g, decl.path, str(self.sql[0]) if len(self.sql) > 0 else "")
