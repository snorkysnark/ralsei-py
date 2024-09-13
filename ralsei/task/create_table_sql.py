from ralsei.connection import ConnectionEnvironment
from ralsei.types import Table

from .base import TaskDef
from .create_table import CreateTableTask


class CreateTableSql(TaskDef):
    sql: str | list[str]
    table: Table
    view: bool = False

    class Impl(CreateTableTask):
        def prepare(self, this: "CreateTableSql"):
            locals = {"table": this.table, "view": this.view}

            self.scripts["Create"] = self.__sql = (
                self.env.render_sql_split(this.sql, **locals)
                if isinstance(this.sql, str)
                else [self.env.render_sql(sql, **locals) for sql in this.sql]
            )
            self._prepare_table(this.table, this.view)

        def _run(self, conn: ConnectionEnvironment):
            conn.sqlalchemy.executescript(self.__sql)
