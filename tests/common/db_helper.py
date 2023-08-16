import psycopg

from ralsei import Table
from ralsei.task.context import MultiConnection
from ralsei.templates import DEFAULT_RENDERER

_SELECT_TEMPLATE = DEFAULT_RENDERER.from_string("SELECT * FROM {{ table }}")


def get_rows(conn: MultiConnection, table: Table):
    with conn.pg().cursor() as curs:
        return curs.execute(_SELECT_TEMPLATE.render(table=table)).fetchall()


def table_exists(conn: MultiConnection, table: Table) -> bool:
    with conn.pg().cursor() as curs:
        try:
            curs.execute(_SELECT_TEMPLATE.render(table=table))
        except psycopg.errors.UndefinedTable:
            return False
    return True
