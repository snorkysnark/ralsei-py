from ralsei.context import PsycopgConn
from ralsei.templates import Table, DEFAULT_RENDERER


class StatusTable:
    def __init__(self, table: Table) -> None:
        params = {"table": table}

        self.__create_table = DEFAULT_RENDERER.render(
            """\
            CREATE TABLE IF NOT EXISTS {{table}}(
                task_name TEXT PRIMARY KEY
            );""",
            params,
        )

        self.__set_done = DEFAULT_RENDERER.render(
            "INSERT INTO {{table}} VALUES (%s);", params
        )
        self.__is_done = DEFAULT_RENDERER.render(
            """\
            SELECT 1 FROM {{table}}
            WHERE task_name = %s;""",
            params,
        )
        self.__unset = DEFAULT_RENDERER.render(
            "DELETE FROM {{table}} WHERE task_name = %s;", params
        )

    def set_done(self, conn: PsycopgConn, task_name: str):
        conn.pg().execute(self.__set_done, [task_name])

    def is_done(self, conn: PsycopgConn, task_name: str) -> bool:
        return conn.pg().execute(self.__is_done, [task_name]).fetchone() is not None

    def unset(self, conn: PsycopgConn, task_name: str) -> None:
        conn.pg().execute(self.__unset, [task_name])
