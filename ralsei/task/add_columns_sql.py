from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei.connection import ConnectionEnvironment
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect
from ralsei import db_actions
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Task


@define(eq=False)
class AddColumnsSql(Task):
    sql: str | list[str]
    table: Table
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict[str, Any] = field(factory=dict)

    class RuntimeData:
        def __init__(self, cfg: AddColumnsSql, env: SqlEnvironment) -> None:
            params = {**cfg.params, "table": cfg.table}

            def render_script() -> (
                tuple[list[sqlalchemy.TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(cfg.sql, str):
                    template_module = env.from_string(cfg.sql).make_module(params)
                    columns: Optional[Sequence[ColumnBase]] = getattr(
                        template_module, "columns", None
                    )

                    return template_module.render_sql_split(), columns
                else:
                    return [env.render_sql(sql, **params) for sql in cfg.sql], None

            self.sql, template_columns = render_script()
            self.columns = [
                column.render(env, **params)
                for column in expect(
                    cfg.columns or template_columns,
                    ValueError("Columns not specified"),
                )
            ]
            self.add_columns = db_actions.AddColumns(env, cfg.table, self.columns)
            self.drop_columns = db_actions.DropColumns(
                env, cfg.table, self.columns, if_exists=True
            )

    _rt: RuntimeData = field(init=False, repr=False)

    def run(self, conn: ConnectionEnvironment):
        self._rt.add_columns(conn)
        conn.executescript(self._rt.sql)
        conn.sqlalchemy.commit()

    def delete(self, conn: ConnectionEnvironment):
        self._rt.drop_columns(conn)
        conn.sqlalchemy.commit()

    def skip(self, conn: sqlalchemy.Connection) -> bool:
        return db_actions.columns_exist(
            conn, self.table, (col.name for col in self._rt.columns)
        )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.path, str(self._rt.add_columns))
