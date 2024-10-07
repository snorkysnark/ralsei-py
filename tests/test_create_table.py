import pytest
from typing import Tuple
from ralsei import Table, CreateTableSql, ConnectionEnvironment
from ralsei.db_actions import table_exists

from tests.db_helper import get_rows


def test_create_table(conn: ConnectionEnvironment):
    table = Table("test_create_table")
    task = CreateTableSql(
        sql=[
            """\
            CREATE TABLE {{table}}(
                foo INT,
                bar TEXT
            );""",
            """\
            INSERT INTO {{table}} VALUES
            (1, 'a'),
            (2, 'b');""",
        ],
        table=table,
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [(1, "a"), (2, "b")]
    task.delete(conn.sqlalchemy)
    assert not table_exists(conn, table)


@pytest.mark.parametrize(
    "flag,expected",
    [
        (True, [("bar",)]),
        (False, []),
    ],
)
def test_create_table_jinja_args(
    conn: ConnectionEnvironment, flag: bool, expected: list[Tuple]
):
    table = Table("test_create_table_jinja_args")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo TEXT
        );
        {%- if flag -%}{%split-%}
        INSERT INTO {{ table }} VALUES ('bar');
        {%- endif %}""",
        table=table,
        locals={"flag": flag},
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == expected
    task.delete(conn.sqlalchemy)
    assert not table_exists(conn, table)


def test_create_table_literal(conn: ConnectionEnvironment):
    table = Table("test_create_table_literal")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{ table }}(
            foo TEXT,
            bar INT
        );
        {%-split-%}
        INSERT INTO {{ table }} VALUES ({{ foo }}, {{ bar }});""",
        table=table,
        locals={"foo": "Ralsei\ncute", "bar": 10},
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [("Ralsei\ncute", 10)]
    task.delete(conn.sqlalchemy)
    assert not table_exists(conn, table)
