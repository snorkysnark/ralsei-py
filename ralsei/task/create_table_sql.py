from __future__ import annotations
from typing import Any
from attrs import define, field
import sqlalchemy

from ralsei.types import Table
from ralsei.jinja import SqlEnvironment
from ralsei.connection.utils import executescript
from ralsei import db_actions
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Task


@define(eq=False)
class CreateTableSql(Task):
    sql: str | list[str]
    table: Table
    view: bool = False
    params: dict[str, Any] = field(factory=dict)

    class RuntimeData:
        def __init__(self, cfg: CreateTableSql, env: SqlEnvironment) -> None:
            params = {**cfg.params, "table": cfg.table, "view": cfg.view}

            self.sql = (
                env.render_sql_split(cfg.sql, **params)
                if isinstance(cfg.sql, str)
                else [env.render_sql(sql, **params) for sql in cfg.sql]
            )
            self.drop_sql = env.render_sql(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=cfg.table,
                view=cfg.view,
            )

    _rt: RuntimeData = field(init=False, repr=False)

    def run(self, conn: sqlalchemy.Connection):
        executescript(conn, self._rt.sql)
        conn.commit()

    def delete(self, conn: sqlalchemy.Connection):
        conn.execute(self._rt.drop_sql)
        conn.commit()

    def skip(self, conn: sqlalchemy.Connection) -> bool:
        return db_actions.table_exists(conn, self.table)

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(
            g, self.path, str(self._rt.sql[0]) if len(self._rt.sql) > 0 else ""
        )
