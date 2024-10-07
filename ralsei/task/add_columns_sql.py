from typing import Optional, Sequence
from sqlalchemy import TextClause

from ralsei.graph import Resolves
from ralsei.types import Table, ColumnBase
from ralsei.utils import expect
from ralsei.connection import ConnectionEnvironment

from .base import TaskDef
from .add_columns import AddColumnsTask


class AddColumnsSql(TaskDef):
    """Adds the specified Columns to an existing Table
    and runs the SQL script to fill them with data

    Variables passed to the template: :py:attr:`~table` |br|
    Columns can be defined in the template itself, using ``{% set columns = [...] %}``

    Example:

        **postprocess.sql**

        .. code-block:: sql

            {% set columns = [Column("name_upper", "TEXT")] -%}

            UPDATE {{table}}
            SET name_upper = UPPER(name);

        **pipeline.py**

        .. code-block:: python

            "postprocess": AddColumnsSql(
                sql=Path("./postprocess.sql").read_text(),
                table=Table("people"),
            )

    Note:
        You can use :py:func:`ralsei.utils.folder` to find SQL files relative to current file
    """

    sql: str | list[str]
    """Sql template strings

    Individual statements must be either separated by ``{%split%}`` tag or pre-split into a list
    """
    table: Resolves[Table]
    """Table to add columns to

    May be the output of another task
    """
    columns: Optional[Sequence[ColumnBase]] = None
    """these column definitions take precedence over those defined in the template"""

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
            columns = expect(
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
