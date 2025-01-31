from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field

from ralsei import db_actions
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

from .base import Task
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

    class RuntimeData:
        def __init__(self, cfg: MapToNewColumns, env: SqlEnvironment) -> None:
            if popped_fields := get_popped_fields(cfg.fn):
                self.popped_fields = set(popped_fields)
            else:
                self.popped_fields = set()

            params = {**cfg.params, "table": cfg.table}
            if cfg.is_done_column:
                params["is_done"] = Identifier(cfg.is_done_column)

            columns = [column.render(env, **params) for column in cfg.columns]
            if cfg.is_done_column:
                columns.append(
                    ValueColumnRendered(cfg.is_done_column, "BOOL DEFAULT FALSE", True)
                )

            self.select = env.render_sql(cfg.select, **params)

            id_fields = cfg.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            if not id_fields:
                raise ValueError(
                    "id_fields not found, must be explicitly provided or inferred from function"
                )

            resumable = bool(cfg.is_done_column)
            self.add_columns = db_actions.AddColumns(
                env, cfg.table, columns, if_not_exists=resumable
            )
            self.update = env.render_sql(
                """\
                UPDATE {{table}} SET
                {{columns | join(',\\n', attribute='set_statement')}}
                WHERE
                {{id_fields | join(' AND ')}};""",
                table=cfg.table,
                columns=columns,
                id_fields=id_fields,
            )
            self.drop_columns = db_actions.DropColumns(
                env, cfg.table, columns, if_exists=True
            )

    _rt: RuntimeData = field(init=False, repr=False)

    def run(self, conn: ConnectionEnvironment):
        self._rt.add_columns(conn)

        for input_row in map(
            lambda row: row._asdict(),
            track(
                conn.execute_with_length_hint(self._rt.select),
                description="Task progress...",
            ),
        ):
            with RowContext.from_input_row(input_row, self._rt.popped_fields):
                conn.execute(self._rt.update, self.fn(**input_row))
                if self.is_done_column:
                    conn.sqlalchemy.commit()

        conn.sqlalchemy.commit()

    def delete(self, conn: ConnectionEnvironment):
        self._rt.drop_columns(conn)
        conn.commit()

    def skip(self, conn: ConnectionEnvironment) -> bool:
        if not db_actions.columns_exist(
            conn.sqlalchemy, self.table, (col.name for col in self.columns)
        ):
            return False
        else:
            return conn.execute(self._rt.select).first() is None

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.path, str(self._rt.add_columns))
