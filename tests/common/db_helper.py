from typing import Optional
import psycopg
from psycopg.sql import Identifier

from ralsei import Table
from ralsei.connection import PsycopgConn
from ralsei.renderer import DEFAULT_RENDERER

_SELECT_TEMPLATE = DEFAULT_RENDERER.from_string(
    """\
    SELECT * FROM {{ table }}
    {%- if order_by %} ORDER BY {{ order_by | sqljoin(', ') }}{% endif %}"""
)


def get_rows(
    conn: PsycopgConn, table: Table, order_by: Optional[list[Identifier]] = None
):
    with conn.pg.cursor() as curs:
        return curs.execute(
            _SELECT_TEMPLATE.render(table=table, order_by=order_by)
        ).fetchall()


def table_exists(conn: PsycopgConn, table: Table) -> bool:
    with conn.pg.cursor() as curs:
        try:
            curs.execute(_SELECT_TEMPLATE.render(table=table))
        except psycopg.errors.UndefinedTable:
            return False
    return True
