from ralsei.connection import ConnectionEnvironment
from ralsei.types import Table

from .base import TaskDef
from .create_table import CreateTableTask


class CreateTableSql(TaskDef):
    """Runs a ``CREATE TABLE`` sql script

    Variables passed to the template: :py:attr:`~table`, :py:attr:`~view`

    Example:

        **unnest.sql**

        .. code-block:: sql

            CREATE TABLE {{table}}(
                id SERIAL PRIMARY KEY,
                name TEXT
            );
            {%-split-%}
            INSERT INTO {{table}}(name)
            SELECT json_array_elements_text(json->'names')
            FROM {{sources}};

        **pipeline.py**

        .. code-block:: python

            "unnest": CreateTableSql(
                sql=Path("./unnest.sql").read_text(),
                table=Table("new_table"),
                locals={"sources": self.outputof("other")},
            )

    Note:
        You can use :py:func:`ralsei.utils.folder` to find SQL files relative to current file
    """

    sql: str | list[str]
    """Sql template strings

    Individual statements must be either separated by ``{%split%}`` tag or pre-split into a list
    """
    table: Table
    """Table being created"""
    view: bool = False
    """whether this is a ``VIEW`` instead of a ``TABLE``"""

    class Impl(CreateTableTask):
        def prepare(self, this: "CreateTableSql"):
            locals = {"table": this.table, "view": this.view}

            self.__sql = (
                self.env.render_sql_split(this.sql, **locals)
                if isinstance(this.sql, str)
                else [self.env.render_sql(sql, **locals) for sql in this.sql]
            )
            self._prepare_table(this.table, this.view)

            self._scripts["Create"] = self.__sql
            self._scripts["Drop"] = self._drop_sql

        def _run(self, conn: ConnectionEnvironment):
            conn.sqlalchemy.executescript(self.__sql)


__all__ = ["CreateTableSql"]
