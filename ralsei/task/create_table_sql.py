from .common import Task, Table, RalseiRenderer, PsycopgConn, checks, merge_params


class CreateTableSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        params: dict = {},
        view: bool = False,
    ) -> None:
        """Runs a `CREATE TABLE` sql script

        Args:
            sql: sql template string
            table: Table being created
            params: parameters passed to the jinja template
            view: whether this is a VIEW instead of a TABLE

        Template:
            Environment variables: `table`, `**params`

        Example:
            ```sql title="unnest.sql"
            CREATE TABLE {{table}}(
                id SERIAL PRIMARY KEY,
                name TEXT
            );

            INSERT INTO {{table}}(name)
            SELECT json_array_elements_text(json->'names')
            FROM {{sources}};
            ```

            ```python title="pipeline.py"
            "unnest": CreateTableSql(
                sql=Path("./unnest.sql").read_text(),
                table=TABLE_names,
                params={"table": TABLE_sources},
            )
            ```
        """

        super().__init__()

        self.__jinja_args = merge_params({"table": table, "view": view}, params)
        self.__sql_raw = sql
        self.__table = table
        self.__is_view = view

    def render(self, renderer: RalseiRenderer) -> None:
        self.scripts["Create"] = self.__sql = renderer.render(
            self.__sql_raw, self.__jinja_args
        )
        self.scripts["Drop"] = self.__drop_sql = renderer.render(
            "DROP {{ ('VIEW' if view else 'TABLE') | sql }} IF EXISTS {{ table }};",
            self.__jinja_args,
        )

    def exists(self, conn: PsycopgConn) -> bool:
        return checks.table_exists(conn, self.__table, self.__is_view)

    def run(self, conn: PsycopgConn) -> None:
        with conn.pg.cursor() as curs:
            curs.execute(self.__sql)

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg.cursor() as curs:
            curs.execute(self.__drop_sql)
