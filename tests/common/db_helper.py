import psycopg
from psycopg import Connection

from ralsei import Table
from ralsei.templates import DEFAULT_RENDERER

_SELECT_TEMPLATE = DEFAULT_RENDERER.from_string("SELECT * FROM {{ table }}")


def get_rows(conn: Connection, table: Table):
    with conn.cursor() as curs:
        return curs.execute(_SELECT_TEMPLATE.render(table=table)).fetchall()


def table_exists(conn: Connection, table: Table) -> bool:
    with conn.cursor() as curs:
        try:
            curs.execute(_SELECT_TEMPLATE.render(table=table))
        except psycopg.errors.UndefinedTable:
            return False
    return True
