from typing import Optional

from ralsei import SqlConnection, Table


def get_rows(conn: SqlConnection, table: Table, order_by: Optional[list[str]] = None):
    return conn.render_execute(
        """\
        SELECT * FROM {{table}}{%if order_by%}
        {%set sep = joiner(', ')-%}
        ORDER BY {%for name in order_by-%}
        {{name | identifier}}
        {%-endfor%}
        {%endif%};""",
        {"table": table, "order_by": order_by},
    ).fetchall()
