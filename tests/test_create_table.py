import pytest
from typing import Tuple
from ralsei import Table, CreateTableSql, Context
from ralsei.actions import table_exists
from common.db_helper import get_rows


def test_create_table(ctx: Context):
    table = Table("test_create_table")
    task = CreateTableSql(
        sql="""
        CREATE TABLE {{table}}(
            foo INT,
            bar TEXT
        );
        {%-split-%}
        INSERT INTO {{table}} VALUES
            (1, 'a'),
            (2, 'b');""",
        table=table,
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == [(1, "a"), (2, "b")]
    task.delete(ctx)
    assert not table_exists(ctx, table)


@pytest.mark.parametrize(
    "flag,expected",
    [
        (True, [("bar",)]),
        (False, []),
    ],
)
def test_create_table_jinja_args(ctx: Context, flag: bool, expected: list[Tuple]):
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
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == expected
    task.delete(ctx)
    assert not table_exists(ctx, table)


def test_create_table_literal(ctx: Context):
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
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == [("Ralsei\ncute", 10)]
    task.delete(ctx)
    assert not table_exists(ctx, table)
