from typing import Optional

import jinja2
import psycopg

from ralsei.templates import Table
import ralsei.params as params
from ralsei.preprocess import format_sql
import ralsei.templates
from .task import Task

DROP_TABLE = "DROP TABLE {{ table }}"


class CreateTableSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        env: Optional[jinja2.Environment] = None,
        extra: dict = {},
    ) -> None:
        env = env or ralsei.templates.default_env()
        jinja_params = params.merge_dicts_no_overwrite({"table": table}, extra)

        self.sql = format_sql(env.from_string(sql).render(jinja_params))
        self.drop_sql = format_sql(env.from_string(DROP_TABLE).render(jinja_params))

    def run(self, conn: psycopg.Connection, env: jinja2.Environment) -> None:
        with conn.cursor() as curs:
            curs.execute(self.sql)

    def delete(self, conn: psycopg.Connection, env: jinja2.Environment) -> None:
        with conn.cursor() as curs:
            curs.execute(self.drop_sql)

    def get_sql_scripts(self) -> dict[str, str]:
        return {"Create": self.sql, "Drop": self.drop_sql}
