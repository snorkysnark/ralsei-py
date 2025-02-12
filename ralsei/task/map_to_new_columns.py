from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field

from ralsei.connection import ConnectionEnvironment
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

from .base import Settled, Task, service, inject
from .add_columns_impl import ImplAddColumns
from .rowcontext import RowContext


@define(eq=False)
class MapToNewColumns(Task):
    select: str
    table: Table
    columns: Sequence[ValueColumnBase]
    fn: OneToOne
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(factory=dict)


@MapToNewColumns.impl
class ImplMapToNewColumns(ImplAddColumns[MapToNewColumns]):
    @inject
    def __init__(
        self, task: Settled[MapToNewColumns], env: SqlEnvironment = service()
    ) -> None:
        popped_fields = get_popped_fields(task.decl.fn)
        self.__popped_fields: set[str] = set(popped_fields) if popped_fields else set()

        params = {**task.decl.params, "table": task.decl.table}
        if task.decl.is_done_column:
            params["is_done"] = Identifier(task.decl.is_done_column)

        columns = [column.render(env, **params) for column in task.decl.columns]
        if task.decl.is_done_column:
            columns.append(
                ValueColumnRendered(
                    task.decl.is_done_column, "BOOL DEFAULT FALSE", True
                )
            )

        self.__resumable = bool(task.decl.is_done_column)
        self.__select = env.render_sql(task.decl.select, **params)
        super().__init__(env, task.decl.table, columns, if_not_exists=self.__resumable)

        id_fields = task.decl.id_fields or (
            [IdColumn(name) for name in popped_fields] if popped_fields else None
        )
        if not id_fields:
            raise ValueError(
                "id_fields not found, must be explicitly provided or inferred from function"
            )

        self.__update = env.render_sql(
            """\
            UPDATE {{table}} SET
            {{columns | join(',\\n', attribute='set_statement')}}
            WHERE
            {{id_fields | join(' AND ')}};""",
            table=task.decl.table,
            columns=columns,
            id_fields=id_fields,
        )

    @inject
    def run(
        self, task: Settled[MapToNewColumns], conn: ConnectionEnvironment = service()
    ):
        self._add_columns(conn)

        for input_row in map(
            lambda row: row._asdict(),
            track(
                conn.execute_with_length_hint(self.__select),
                description="Task progress...",
            ),
        ):
            with RowContext.from_input_row(input_row, self.__popped_fields):
                conn.execute(self.__update, task.decl.fn(**input_row))
                if self.__resumable:
                    conn.commit()

        conn.commit()

    @inject
    def skip(
        self, task: Settled[MapToNewColumns], conn: ConnectionEnvironment = service()
    ) -> bool:
        if not super().skip(task):
            return False
        else:
            return not self.__resumable or conn.execute(self.__select).first() is None

    def visualize(self, task: Settled[MapToNewColumns], g: VisualGraph) -> VisualNode:
        return WindowNode(g, task.path, str(self._add_columns))
