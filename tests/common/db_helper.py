import psycopg
from psycopg import Connection

from ralsei import Table


def get_rows(conn: Connection, table: Table):
    with conn.cursor() as curs:
        return curs.execute(f"SELECT * FROM {table}").fetchall()


def table_exists(conn: Connection, table: Table) -> bool:
    with conn.cursor() as curs:
        try:
            curs.execute(f"SELECT * FROM {table}")
        except psycopg.errors.UndefinedTable:
            return False
    return True
