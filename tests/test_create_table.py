from typing import Tuple
from psycopg import Connection
import pytest

from ralsei import Table, CreateTableSql
from common.db_helper import get_rows, table_exists


def test_create_table(conn: Connection):
    table = Table("test_create_table")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo INT,
            bar TEXT
        );

        INSERT INTO {{ table }} VALUES
            (1, 'a'),
            (2, 'b');
        ;
        """,
        table=table,
    )

    task.run(conn)
    assert get_rows(conn, table) == [(1, "a"), (2, "b")]
    task.delete(conn)
    assert not table_exists(conn, table)


@pytest.mark.parametrize(
    "flag,expected",
    [
        (True, [("bar",)]),
        (False, []),
    ],
)
def test_create_table_jinja_args(conn: Connection, flag: bool, expected: list[Tuple]):
    table = Table("test_create_table_jinja_args")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo TEXT
        );

        {% if flag %}
            INSERT INTO {{ table }} VALUES ('bar');
        {% endif %}
        """,
        table=table,
        jinja_args={"flag": flag},
    )

    task.run(conn)
    assert get_rows(conn, table) == expected
    task.delete(conn)
    assert not table_exists(conn, table)


@pytest.mark.parametrize(
    "foo",
    ["first", "second"],
)
def test_create_table_sql_args(conn: Connection, foo: str):
    table = Table("test_create_table_jinja_args")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo TEXT
        );

        INSERT INTO {{ table }} VALUES (%(foo)s);
        """,
        table=table,
        sql_args={"foo": foo},
    )

    task.run(conn)
    assert get_rows(conn, table) == [(foo,)]
    task.delete(conn)
    assert not table_exists(conn, table)
