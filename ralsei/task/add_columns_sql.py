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

from .base import Impl, Task


@define(eq=False)
class AddColumnsSql(Task["ImplAddColumnsSql"]):
    sql: str | list[str]
    table: Table
    columns: Optional[Sequence[ColumnBase]] = None
    params: dict[str, Any] = field(factory=dict)

    impl: ImplAddColumnsSql = field(init=False, repr=False)


class ImplAddColumnsSql(Impl[AddColumnsSql]):
    def __init__(self, decl: AddColumnsSql, env: SqlEnvironment) -> None:
        params = {**decl.params, "table": decl.table}

        def render_script() -> (
            tuple[list[sqlalchemy.TextClause], Optional[Sequence[ColumnBase]]]
        ):
            if isinstance(decl.sql, str):
                template_module = env.from_string(decl.sql).make_module(params)
                columns: Optional[Sequence[ColumnBase]] = getattr(
                    template_module, "columns", None
                )

                return template_module.render_sql_split(), columns
            else:
                return [env.render_sql(sql, **params) for sql in decl.sql], None

        self.sql, template_columns = render_script()
        self.columns = [
            column.render(env, **params)
            for column in expect(
                decl.columns or template_columns,
                ValueError("Columns not specified"),
            )
        ]
        self.add_columns = db_actions.AddColumns(env, decl.table, self.columns)
        self.drop_columns = db_actions.DropColumns(
            env, decl.table, self.columns, if_exists=True
        )

    def run(self, decl: AddColumnsSql, context: DIContext):
        conn = context.get(ConnectionEnvironment)

        self.add_columns(conn)
        conn.executescript(self.sql)
        conn.commit()

    def delete(self, decl: AddColumnsSql, context: DIContext):
        conn = context.get(ConnectionEnvironment)

        self.drop_columns(conn)
        conn.commit()

    def skip(self, decl: AddColumnsSql, context: DIContext):
        return db_actions.columns_exist(
            context.get(sqlalchemy.Connection),
            decl.table,
            (col.name for col in self.columns),
        )

    def visualize(self, decl: AddColumnsSql, g: VisualGraph) -> VisualNode:
        return WindowNode(g, decl.path, str(self.add_columns))
