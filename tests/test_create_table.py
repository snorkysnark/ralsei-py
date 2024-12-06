import pytest
from typing import Tuple

import sqlalchemy

from ralsei import Table, CreateTableSql, ConnectionEnvironment
from ralsei.db_actions import table_exists
from ralsei.app import Ralsei

from tests.db_helper import get_rows


def test_create_table(app: Ralsei):
    table = Table("test_create_table")

    with app.init_context() as init:
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
        ).create(init)

    with app.runtime_context() as runtime:
        runtime.execute(task.run)
        assert get_rows(runtime.get(ConnectionEnvironment), table) == [
            (1, "a"),
            (2, "b"),
        ]

        runtime.execute(task.output.delete)
        assert not table_exists(runtime.get(sqlalchemy.Connection), table)


@pytest.mark.parametrize(
    "flag,expected",
    [
        (True, [("bar",)]),
        (False, []),
    ],
)
def test_create_table_jinja_args(app: Ralsei, flag: bool, expected: list[Tuple]):
    table = Table("test_create_table_jinja_args")

    with app.init_context() as init:
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
        ).create(init)

    with app.runtime_context() as runtime:
        runtime.execute(task.run)
        assert get_rows(runtime.get(ConnectionEnvironment), table) == expected
        runtime.execute(task.output.delete)
        assert not table_exists(runtime.get(sqlalchemy.Connection), table)


def test_create_table_literal(app: Ralsei):
    table = Table("test_create_table_literal")

    with app.init_context() as init:
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
        ).create(init)

    with app.runtime_context() as runtime:
        runtime.execute(task.run)
        assert get_rows(runtime.get(ConnectionEnvironment), table) == [
            ("Ralsei\ncute", 10)
        ]
        runtime.execute(task.output.delete)
        assert not table_exists(runtime.get(sqlalchemy.Connection), table)
