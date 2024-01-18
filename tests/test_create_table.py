import pytest
from typing import Tuple
from ralsei import Table, CreateTableSql, JinjaSqlConnection
from ralsei.db_actions import table_exists
from common.db_helper import get_rows


def test_create_table(jsql: JinjaSqlConnection):
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
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == [(1, "a"), (2, "b")]
    task.delete(jsql)
    assert not table_exists(jsql, table)


@pytest.mark.parametrize(
    "flag,expected",
    [
        (True, [("bar",)]),
        (False, []),
    ],
)
def test_create_table_jinja_args(
    jsql: JinjaSqlConnection, flag: bool, expected: list[Tuple]
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
        params={"flag": flag},
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == expected
    task.delete(jsql)
    assert not table_exists(jsql, table)


def test_create_table_literal(jsql: JinjaSqlConnection):
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
        params={"foo": "Ralsei\ncute", "bar": 10},
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == [("Ralsei\ncute", 10)]
    task.delete(jsql)
    assert not table_exists(jsql, table)
