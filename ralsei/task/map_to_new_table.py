from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.injector import DIContext, inject, service
from ralsei.jinja import SqlEnvironment
from ralsei.types import (
    Table,
    ValueColumnBase,
    IdColumn,
    Identifier,
    ToSql,
    ValueColumnRendered,
    Sql,
    ColumnRendered,
)
from ralsei.viz import VisualGraph, VisualNode, WindowNode
from ralsei.wrappers import OneToMany, get_popped_fields
from ralsei.console import track

from .base import Settled, Task
from .create_table_impl import ImplCreateTable
from .rowcontext import RowContext


@define(eq=False)
class MarkerScripts:
    add: db_actions.AddColumns
    set_marker: sqlalchemy.TextClause
    drop: db_actions.DropColumns


@define(eq=False)
class MapToNewTable(Task):
    table: Table
    columns: Sequence[str | ValueColumnBase]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[Table] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(factory=dict)


@MapToNewTable.impl
class ImplMapToNewTable(ImplCreateTable[MapToNewTable]):
    @inject
    def __init__(
        self, task: Settled[MapToNewTable], env: SqlEnvironment = service()
    ) -> None:
        super().__init__(task, env, task.decl.table)

        if popped_fields := get_popped_fields(task.decl.fn):
            self.popped_fields = set(popped_fields)
        else:
            self.popped_fields = set()

        params = {
            **task.decl.params,
            "table": task.decl.table,
            "source": task.decl.source_table,
        }
        if task.decl.is_done_column:
            params["is_done"] = Identifier(task.decl.is_done_column)

        definitions: list[ToSql] = []
        insert_columns: list[ValueColumnRendered] = []
        for column in task.decl.columns:
            if isinstance(column, str):
                rendered = Sql(env.render(column, **params))
                definitions.append(rendered)
            else:
                rendered = column.render(env, **params)
                insert_columns.append(rendered)
                definitions.append(rendered.definition)

        self.select = (
            env.render_sql(task.decl.select, **params) if task.decl.select else None
        )
        self.create_table = env.render_sql(
            """\
            CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                {{ definitions | join(',\\n    ') }}
            );""",
            table=task.decl.table,
            definitions=definitions,
            if_not_exists=task.decl.is_done_column is not None,
        )
        self.insert = env.render_sql(
            """\
            INSERT INTO {{table}}(
                {{ columns | join(',\\n    ', attribute='identifier') }}
            )
            VALUES (
                {{ columns | join(',\\n    ', attribute='value') }}
            );""",
            table=task.decl.table,
            columns=insert_columns,
        )

        self.marker_scripts: Optional[MarkerScripts] = None
        if task.decl.is_done_column:
            if not task.decl.source_table:
                raise ValueError(
                    "Cannot create is_done_column when source_table is None"
                )
            if self.select is None:
                raise ValueError("'select' cannot be empty if using is_done_column")

            id_fields = task.decl.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            if not id_fields:
                ValueError("Must provide id_fields if using is_done_column")

            is_done_column = ColumnRendered(
                task.decl.is_done_column, "BOOL DEFAULT FALSE"
            )

            self.marker_scripts = MarkerScripts(
                add=db_actions.AddColumns(
                    env, task.decl.source_table, [is_done_column], if_not_exists=True
                ),
                set_marker=env.render_sql(
                    """\
                    UPDATE {{source}}
                    SET {{is_done}} = TRUE
                    WHERE {{id_fields | join(' AND ')}};""",
                    source=task.decl.source_table,
                    is_done=is_done_column.identifier,
                    id_fields=id_fields,
                ),
                drop=db_actions.DropColumns(
                    env, task.decl.source_table, [is_done_column], if_exists=True
                ),
            )

    @inject
    def run(self, conn: ConnectionEnvironment = service()):
        conn.execute(self.create_table)
        if marker := self.marker_scripts:
            marker.add(conn)

        def iter_input_rows(select: sqlalchemy.TextClause):
            for input_row in map(
                lambda row: row._asdict(),
                track(
                    conn.execute_with_length_hint(select),
                    description="Task progress...",
                ),
            ):
                yield input_row

                if marker := self.marker_scripts:
                    conn.execute(marker.set_marker, input_row)
                    conn.commit()

        for input_row in (
            iter_input_rows(self.select) if self.select is not None else [{}]
        ):
            with RowContext.from_input_row(input_row, self.popped_fields):
                for output_row in self.decl.fn(**input_row):
                    conn.sqlalchemy.execute(self.insert, output_row)

        conn.sqlalchemy.commit()

    @inject
    def delete(self, context: DIContext, conn: ConnectionEnvironment = service()):
        if marker := self.marker_scripts:
            marker.drop(conn)

        super().delete(context)

    @inject
    def skip(self, context: DIContext, conn: ConnectionEnvironment = service()) -> bool:
        if not super().skip(context):
            return False
        else:
            # Check that task has no more inputs
            return (
                self.select is None
                or not self.marker_scripts
                or conn.execute(self.select).first() is None
            )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.task.path, str(self.create_table))
