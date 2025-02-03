from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.injector import DIContext
from ralsei.jinja import SqlEnvironment
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    Identifier,
    ValueColumnRendered,
)
from ralsei.viz import VisualGraph, VisualNode, WindowNode
from ralsei.wrappers import OneToOne, get_popped_fields
from ralsei.console import track

from .base import Impl, Task
from .rowcontext import RowContext


@define(eq=False)
class MapToNewColumns(Task["ImplMapToNewColumns"]):
    select: str
    table: Table
    columns: Sequence[ValueColumnBase]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(factory=dict)

    impl: ImplMapToNewColumns = field(init=False, repr=False)


class ImplMapToNewColumns(Impl[MapToNewColumns]):
    def __init__(self, decl: MapToNewColumns, env: SqlEnvironment) -> None:
        if popped_fields := get_popped_fields(decl.fn):
            self.popped_fields = set(popped_fields)
        else:
            self.popped_fields = set()

        params = {**decl.params, "table": decl.table}
        if decl.is_done_column:
            params["is_done"] = Identifier(decl.is_done_column)

        columns = [column.render(env, **params) for column in decl.columns]
        if decl.is_done_column:
            columns.append(
                ValueColumnRendered(decl.is_done_column, "BOOL DEFAULT FALSE", True)
            )

        self.select = env.render_sql(decl.select, **params)

        id_fields = decl.id_fields or (
            [IdColumn(name) for name in popped_fields] if popped_fields else None
        )
        if not id_fields:
            raise ValueError(
                "id_fields not found, must be explicitly provided or inferred from function"
            )

        resumable = bool(decl.is_done_column)
        self.add_columns = db_actions.AddColumns(
            env, decl.table, columns, if_not_exists=resumable
        )
        self.update = env.render_sql(
            """\
            UPDATE {{table}} SET
            {{columns | join(',\\n', attribute='set_statement')}}
            WHERE
            {{id_fields | join(' AND ')}};""",
            table=decl.table,
            columns=columns,
            id_fields=id_fields,
        )
        self.drop_columns = db_actions.DropColumns(
            env, decl.table, columns, if_exists=True
        )

    def run(self, decl: MapToNewColumns, context: DIContext):
        conn = context.get(ConnectionEnvironment)
        self.add_columns(conn)

        for input_row in map(
            lambda row: row._asdict(),
            track(
                conn.execute_with_length_hint(self.select),
                description="Task progress...",
            ),
        ):
            with RowContext.from_input_row(input_row, self.popped_fields):
                conn.execute(self.update, decl.fn(**input_row))
                if decl.is_done_column:
                    conn.sqlalchemy.commit()

        conn.sqlalchemy.commit()

    def delete(self, decl: MapToNewColumns, context: DIContext):
        conn = context.get(ConnectionEnvironment)
        self.drop_columns(conn)
        conn.commit()

    def skip(self, decl: MapToNewColumns, context: DIContext) -> bool:
        conn = context.get(ConnectionEnvironment)
        if not db_actions.columns_exist(
            conn.sqlalchemy, decl.table, (col.name for col in decl.columns)
        ):
            return False
        else:
            return conn.execute(self.select).first() is None

    def visualize(self, decl: MapToNewColumns, g: VisualGraph) -> VisualNode:
        return WindowNode(g, decl.path, str(self.add_columns))
