from typing import Optional

from .common import (
    Task,
    Table,
    Column,
    PsycopgConn,
    merge_params,
    checks,
    renderer,
)


class AddColumnsSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        columns: Optional[list[Column]] = None,
        params: dict = {},
    ) -> None:
        """
        Adds the specified Columns to an existing Table
        and runs the SQL script to fill them with data

        Args:
            sql: sql template string
            table: Table to add columns to
            columns: these column definitions take precedence over those defined in the template
            params: parameters passed to the jinja template

        Template:
            Environment variables: `table`, `**params`

            Columns can be defined in the template itself,
            using `{% set columns = [...] %}`

        Example:
            ```sql title="postprocess.sql"
            {% set columns = [Column("name_upper", "TEXT")] %}

            UPDATE {{table}}
            SET name_upper = UPPER(name);
            ```

            ```python title="pipeline.py"
            "postprocess": AddColumnsSql(
                sql=Path("./postprocess.sql").read_text(),
                table=TABLE_people,
            )
            ```
        """

        super().__init__()

        jinja_args = merge_params({"table": table}, params)

        script_module = renderer.from_string(sql).make_module(jinja_args)

        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = script_module.getattr("columns", None)
            if columns is None:
                raise ValueError("Columns not specified")
        self.__column_names = list(map(lambda col: col.name, columns))

        rendered_columns = list(
            map(lambda col: col.render(renderer, jinja_args), columns)
        )

        self.scripts["Add columns"] = self.__add_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\n') }};""",
            merge_params(
                jinja_args,
                {"columns": map(lambda col: col.add(False), rendered_columns)},
            ),
        )
        self.scripts["Main"] = self.__sql = script_module.render()
        self.scripts["Drop columns"] = self.__drop_columns = renderer.render(
            """\
            ALTER TABLE {{ table }}
            {{ columns | sqljoin(',\n') }};""",
            merge_params(
                jinja_args,
                {"columns": map(lambda col: col.drop(True), rendered_columns)},
            ),
        )

        self.__table = table

    def exists(self, conn: PsycopgConn) -> bool:
        return checks.columns_exist(conn, self.__table, self.__column_names)

    def run(self, conn: PsycopgConn) -> None:
        with conn.pg.cursor() as curs:
            curs.execute(self.__add_columns)
            curs.execute(self.__sql)

    def delete(self, conn: PsycopgConn) -> None:
        with conn.pg.cursor() as curs:
            curs.execute(self.__drop_columns)
