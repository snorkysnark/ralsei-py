from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei.connection import ConnectionEnvironment
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Settled, Task, inject, service
from .add_columns_impl import ImplAddColumns


@define(eq=False)
class AddColumnsSql(Task):
    sql: str | list[str]
    table: Table
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict[str, Any] = field(factory=dict)


@AddColumnsSql.impl
class ImplAddColumnsSql(ImplAddColumns[AddColumnsSql]):
    @inject
    def __init__(self, task: Settled[AddColumnsSql], env: SqlEnvironment = service()):
        params = {**task.cfg.params, "table": task.cfg.table}

        def render_script() -> (
            tuple[list[sqlalchemy.TextClause], Optional[Sequence[ColumnBase]]]
        ):
            if isinstance(task.cfg.sql, str):
                template_module = env.from_string(task.cfg.sql).make_module(params)
                columns: Optional[Sequence[ColumnBase]] = getattr(
                    template_module, "columns", None
                )

                return template_module.render_sql_split(), columns
            else:
                return [env.render_sql(sql, **params) for sql in task.cfg.sql], None

        self.__sql, template_columns = render_script()
        columns = [
            column.render(env, **params)
            for column in expect(
                task.cfg.columns or template_columns,
                ValueError("Columns not specified"),
            )
        ]

        super().__init__(task, task.cfg.table, columns)

    @inject
    def run(self, conn: ConnectionEnvironment = service()):
        self._add_columns(conn)
        conn.executescript(self.__sql)
        conn.commit()

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.task.path, str(self._add_columns))
