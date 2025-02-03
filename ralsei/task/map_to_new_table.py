from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
from ralsei.injector import DIContext
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

from .base import Impl, Task
from .rowcontext import RowContext


@define(eq=False)
class MarkerScripts:
    add: db_actions.AddColumns
    set_marker: sqlalchemy.TextClause
    drop: db_actions.DropColumns


@define(eq=False)
class MapToNewTable(Task["ImplMapToNewTable"]):
    table: Table
    columns: Sequence[str | ValueColumnBase]
    fn: OneToMany
    select: Optional[str] = None
    source_table: Optional[Table] = None
    is_done_column: Optional[str] = None
    id_fields: Optional[list[IdColumn]] = None
    params: dict[str, Any] = field(factory=dict)

    impl: ImplMapToNewTable = field(init=False, repr=False)


class ImplMapToNewTable(Impl[MapToNewTable]):
    def __init__(self, decl: MapToNewTable, env: SqlEnvironment) -> None:
        if popped_fields := get_popped_fields(decl.fn):
            self.popped_fields = set(popped_fields)
        else:
            self.popped_fields = set()

        params = {**decl.params, "table": decl.table, "source": decl.source_table}
        if decl.is_done_column:
            params["is_done"] = Identifier(decl.is_done_column)

        definitions: list[ToSql] = []
        insert_columns: list[ValueColumnRendered] = []
        for column in decl.columns:
            if isinstance(column, str):
                rendered = Sql(env.render(column, **params))
                definitions.append(rendered)
            else:
                rendered = column.render(env, **params)
                insert_columns.append(rendered)
                definitions.append(rendered.definition)

        self.select = env.render_sql(decl.select, **params) if decl.select else None
        self.create_table = env.render_sql(
            """\
            CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                {{ definitions | join(',\\n    ') }}
            );""",
            table=decl.table,
            definitions=definitions,
            if_not_exists=decl.is_done_column is not None,
        )
        self.drop_table = env.render_sql("DROP {{table}} IF EXISTS", table=decl.table)
        self.insert = env.render_sql(
            """\
            INSERT INTO {{table}}(
                {{ columns | join(',\\n    ', attribute='identifier') }}
            )
            VALUES (
                {{ columns | join(',\\n    ', attribute='value') }}
            );""",
            table=decl.table,
            columns=insert_columns,
        )

        self.marker_scripts: Optional[MarkerScripts] = None
        if decl.is_done_column:
            if not decl.source_table:
                raise ValueError(
                    "Cannot create is_done_column when source_table is None"
                )
            if self.select is None:
                raise ValueError("'select' cannot be empty if using is_done_column")

            id_fields = decl.id_fields or (
                [IdColumn(name) for name in popped_fields] if popped_fields else None
            )
            if not id_fields:
                ValueError("Must provide id_fields if using is_done_column")

            is_done_column = ColumnRendered(decl.is_done_column, "BOOL DEFAULT FALSE")

            self.marker_scripts = MarkerScripts(
                add=db_actions.AddColumns(
                    env, decl.source_table, [is_done_column], if_not_exists=True
                ),
                set_marker=env.render_sql(
                    """\
                    UPDATE {{source}}
                    SET {{is_done}} = TRUE
                    WHERE {{id_fields | join(' AND ')}};""",
                    source=decl.source_table,
                    is_done=is_done_column.identifier,
                    id_fields=id_fields,
                ),
                drop=db_actions.DropColumns(
                    env, decl.source_table, [is_done_column], if_exists=True
                ),
            )

    def run(self, decl: MapToNewTable, context: DIContext):
        conn = context.get(ConnectionEnvironment)

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
                for output_row in decl.fn(**input_row):
                    conn.sqlalchemy.execute(self.insert, output_row)

        conn.sqlalchemy.commit()

    def delete(self, decl: MapToNewTable, context: DIContext):
        conn = context.get(ConnectionEnvironment)

        if marker := self.marker_scripts:
            marker.drop(conn)

        conn.execute(self.drop_table)
        conn.commit()

    def skip(self, decl: MapToNewTable, context: DIContext) -> bool:
        conn = context.get(ConnectionEnvironment)

        if not db_actions.table_exists(conn.sqlalchemy, decl.table):
            return False
        else:
            # Check that task has no more inputs
            return self.select is not None and conn.execute(self.select).first() is None

    def visualize(self, decl: MapToNewTable, g: VisualGraph) -> VisualNode:
        return WindowNode(g, decl.path, str(self.create_table))
