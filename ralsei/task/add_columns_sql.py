from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei.connection import ConnectionEnvironment
from ralsei.injector import DIContext
from ralsei.jinja import SqlEnvironment
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect
from ralsei import db_actions
from ralsei.viz import VisualGraph, VisualNode, WindowNode

from .base import Settled, Task
from .add_columns import AddColumnsBase


@define(eq=False)
class AddColumnsSql(Task):
    sql: str | list[str]
    table: Table
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict[str, Any] = field(factory=dict)


@AddColumnsSql.impl
class ImplAddColumnsSql(AddColumnsBase[AddColumnsSql]):
    def __init__(self, task: Settled[AddColumnsSql], context: DIContext):
        env = context.get(SqlEnvironment)
        params = {**task.decl.params, "table": task.decl.table}

        def render_script() -> (
            tuple[list[sqlalchemy.TextClause], Optional[Sequence[ColumnBase]]]
        ):
            if isinstance(task.decl.sql, str):
                template_module = env.from_string(task.decl.sql).make_module(params)
                columns: Optional[Sequence[ColumnBase]] = getattr(
                    template_module, "columns", None
                )

                return template_module.render_sql_split(), columns
            else:
                return [env.render_sql(sql, **params) for sql in task.decl.sql], None

        self.__sql, template_columns = render_script()
        columns = [
            column.render(env, **params)
            for column in expect(
                task.decl.columns or template_columns,
                ValueError("Columns not specified"),
            )
        ]

        super().__init__(task, env, task.decl.table, columns)

    def run(self, context: DIContext):
        conn = context.get(ConnectionEnvironment)

        self._add_columns(conn)
        conn.executescript(self.__sql)
        conn.commit()

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.task.path, str(self._add_columns))
