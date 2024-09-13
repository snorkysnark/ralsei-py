from typing import Optional, Sequence
from sqlalchemy import TextClause

from ralsei.graph import Resolves
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect_optional
from ralsei.connection import ConnectionEnvironment

from .base import TaskDef
from .add_columns import AddColumnsTask


class AddColumnsSql(TaskDef):
    sql: str | list[str]
    table: Resolves[Table]
    columns: Optional[Sequence[ColumnBase]] = None

    class Impl(AddColumnsTask):
        def prepare(self, this: "AddColumnsSql"):
            table = self.resolve(this.table)

            def render_script() -> (
                tuple[list[TextClause], Optional[Sequence[ColumnBase]]]
            ):
                if isinstance(this.sql, str):
                    template_module = self.env.from_string(this.sql).make_module(
                        {"table": table}
                    )
                    columns: Optional[Sequence[ColumnBase]] = getattr(
                        template_module, "columns", None
                    )

                    return template_module.render_sql_split(), columns
                else:
                    return [
                        self.env.render_sql(sql, table=table) for sql in this.sql
                    ], None

            self.__sql, template_columns = render_script()
            columns = expect_optional(
                this.columns or template_columns, ValueError("Columns not specified")
            )

            self._prepare_columns(table, columns)

            self._scripts["Add Columns"] = self._add_columns
            self._scripts["Main"] = self.__sql
            self._scripts["Drop Columns"] = self._drop_columns

        def _run(self, conn: ConnectionEnvironment):
            self._add_columns(conn)
            conn.sqlalchemy.executescript(self.__sql)


__all__ = ["AddColumnsSql"]
