from __future__ import annotations
from typing import Any
from attrs import define, field
import sqlalchemy

from .base import Task


@define(eq=False)
class CreateTableSql(Task):
    sql: str | list[str]
    table: Table
    view: bool = False
    params: dict[str, Any] = field(factory=dict)

    class Impl:
        def __init__(self, cfg: CreateTableSql, env: SqlEnvironment) -> None:
            params = {**cfg.params, "table": cfg.table, "view": cfg.view}

            self.table = cfg.table
            self._sql = (
                env.render_sql_split(cfg.sql, **params)
                if isinstance(cfg.sql, str)
                else [env.render_sql(sql, **params) for sql in cfg.sql]
            )
            self._drop_sql = env.render_sql(
                "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
                table=cfg.table,
                view=cfg.view,
            )

        def run(self, conn: sqlalchemy.Connection):
            executescript(conn, self._sql)
            conn.commit()

        def delete(self, conn: sqlalchemy.Connection):
            conn.execute(self._drop_sql)
            conn.commit()

        def exists(self, conn: ConnectionEnvironment) -> bool:
            return db_actions.table_exists(conn.sqlalchemy, self.table)
