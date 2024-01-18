from typing import Optional

from ralsei import JinjaSqlConnection, Table


def get_rows(
    jsql: JinjaSqlConnection, table: Table, order_by: Optional[list[str]] = None
):
    return jsql.render_execute(
        """\
        SELECT * FROM {{table}}{%if order_by%}
        {%set sep = joiner(', ')-%}
        ORDER BY {%for name in order_by-%}
        {{name | identifier}}
        {%-endfor%}
        {%endif%};""",
        {"table": table, "order_by": order_by},
    ).fetchall()
