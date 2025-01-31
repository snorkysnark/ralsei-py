from __future__ import annotations
from typing import Any, Optional, Sequence
from attrs import define, field
import sqlalchemy

from ralsei import db_actions
from ralsei.connection import ConnectionEnvironment
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

from .base import Task
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

    class RuntimeData:
        def __init__(self, cfg: MapToNewTable, env: SqlEnvironment) -> None:
            if popped_fields := get_popped_fields(cfg.fn):
                self.popped_fields = set(popped_fields)
            else:
                self.popped_fields = set()

            params = {**cfg.params, "table": cfg.table, "source": cfg.source_table}
            if cfg.is_done_column:
                params["is_done"] = Identifier(cfg.is_done_column)

            definitions: list[ToSql] = []
            insert_columns: list[ValueColumnRendered] = []
            for column in cfg.columns:
                if isinstance(column, str):
                    rendered = Sql(env.render(column, **params))
                    definitions.append(rendered)
                else:
                    rendered = column.render(env, **params)
                    insert_columns.append(rendered)
                    definitions.append(rendered.definition)

            self.select = env.render_sql(cfg.select, **params) if cfg.select else None
            self.create_table = env.render_sql(
                """\
                CREATE TABLE {% if if_not_exists %}IF NOT EXISTS {% endif %}{{ table }}(
                    {{ definitions | join(',\\n    ') }}
                );""",
                table=cfg.table,
                definitions=definitions,
                if_not_exists=cfg.is_done_column is not None,
            )
            self.drop_table = env.render_sql(
                "DROP {{table}} IF EXISTS", table=cfg.table
            )
            self.insert = env.render_sql(
                """\
                INSERT INTO {{table}}(
                    {{ columns | join(',\\n    ', attribute='identifier') }}
                )
                VALUES (
                    {{ columns | join(',\\n    ', attribute='value') }}
                );""",
                table=cfg.table,
                columns=insert_columns,
            )

            self.marker_scripts: Optional[MarkerScripts] = None
            if cfg.is_done_column:
                if not cfg.source_table:
                    raise ValueError(
                        "Cannot create is_done_column when source_table is None"
                    )
                if self.select is None:
                    raise ValueError("'select' cannot be empty if using is_done_column")

                id_fields = cfg.id_fields or (
                    [IdColumn(name) for name in popped_fields]
                    if popped_fields
                    else None
                )
                if not id_fields:
                    ValueError("Must provide id_fields if using is_done_column")

                is_done_column = ColumnRendered(
                    cfg.is_done_column, "BOOL DEFAULT FALSE"
                )

                self.marker_scripts = MarkerScripts(
                    add=db_actions.AddColumns(
                        env, cfg.source_table, [is_done_column], if_not_exists=True
                    ),
                    set_marker=env.render_sql(
                        """\
                        UPDATE {{source}}
                        SET {{is_done}} = TRUE
                        WHERE {{id_fields | join(' AND ')}};""",
                        source=cfg.source_table,
                        is_done=is_done_column.identifier,
                        id_fields=id_fields,
                    ),
                    drop=db_actions.DropColumns(
                        env, cfg.source_table, [is_done_column], if_exists=True
                    ),
                )

    _rt: RuntimeData = field(init=False, repr=False)

    def run(self, conn: ConnectionEnvironment):
        conn.execute(self._rt.create_table)
        if marker := self._rt.marker_scripts:
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

                if marker := self._rt.marker_scripts:
                    conn.execute(marker.set_marker, input_row)
                    conn.commit()

        for input_row in (
            iter_input_rows(self._rt.select) if self._rt.select is not None else [{}]
        ):
            with RowContext.from_input_row(input_row, self._rt.popped_fields):
                for output_row in self.fn(**input_row):
                    conn.sqlalchemy.execute(self._rt.insert, output_row)

        conn.sqlalchemy.commit()

    def delete(self, conn: ConnectionEnvironment):
        if marker := self._rt.marker_scripts:
            marker.drop(conn)

        conn.execute(self._rt.drop_table)
        conn.commit()

    def skip(self, conn: ConnectionEnvironment) -> bool:
        if not db_actions.table_exists(conn.sqlalchemy, self.table):
            return False
        else:
            # Check that task has no more inputs
            return (
                self._rt.select is not None
                and conn.execute(self._rt.select).first() is None
            )

    def visualize(self, g: VisualGraph) -> VisualNode:
        return WindowNode(g, self.path, str(self._rt.create_table))
