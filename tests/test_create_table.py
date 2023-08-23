from typing import Tuple
import pytest

from ralsei import Table, CreateTableSql
from common.db_helper import get_rows, table_exists
from ralsei.connection import PsycopgConn
from ralsei.renderer import RalseiRenderer


def test_create_table(conn: PsycopgConn):
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
    task.render(RalseiRenderer())

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
def test_create_table_jinja_args(conn: PsycopgConn, flag: bool, expected: list[Tuple]):
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
        params={"flag": flag},
    )
    task.render(RalseiRenderer())

    task.run(conn)
    assert get_rows(conn, table) == expected
    task.delete(conn)
    assert not table_exists(conn, table)


def test_create_table_literal(conn: PsycopgConn):
    table = Table("test_create_table_literal")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo TEXT,
            bar INT
        );
        INSERT INTO {{ table }} VALUES ({{ foo }}, {{ bar }});
        """,
        table=table,
        params={"foo": "Ralsei\ncute", "bar": 10},
    )
    task.render(RalseiRenderer())

    task.run(conn)
    assert get_rows(conn, table) == [("Ralsei\ncute", 10)]
    task.delete(conn)
    assert not table_exists(conn, table)
