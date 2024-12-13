import pytest
from ralsei import (
    App,
    ConnectionEnvironment,
    Table,
    MapToNewTable,
    ValueColumn,
    Sql,
    compose,
    pop_id_fields,
)
from ralsei.db_actions import table_exists
import sqlalchemy

from tests.db_helper import get_rows
from tests.error import IntentionallyFailError


def test_map_new_table_noselect(app: App):
    def make_rows():
        yield {"foo": 1, "bar": "a"}
        yield {"foo": 2, "bar": "b"}

    table = Table("test_map_table")

    with app.init_context() as init:
        task = MapToNewTable(
            table=table,
            columns=[
                "id {{ utils.autoincrement_primary_key() }}",
                ValueColumn("foo", "INT"),
                ValueColumn("bar", "TEXT"),
            ],
            fn=make_rows,
        ).create(init)

    with app.runtime_context() as runtime:
        runtime.execute(task.run)
        assert get_rows(runtime.get(ConnectionEnvironment), table) == [
            (1, 1, "a"),
            (2, 2, "b"),
        ]
        runtime.execute(task.output.delete)
        assert not table_exists(runtime.get(sqlalchemy.Connection), table)


def test_map_table_jinja(app: App):
    def double(foo: int):
        yield {"foo": foo * 2}

    table_source = Table("source_args")
    table = Table("test_map_table_select")

    with app.init_context() as init:
        task = MapToNewTable(
            table=table,
            source_table=table_source,
            select="SELECT * FROM {{source}}",
            columns=[
                "year INT DEFAULT {{year}}",
                ValueColumn("foo", "{{foo_type}}"),
            ],
            fn=double,
            params={"year": 2015, "foo_type": Sql("INT")},
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        conn.render_executescript(
            [
                """\
                CREATE TABLE {{table}}(
                    foo INT
                );""",
                "INSERT INTO {{table}} VALUES (2), (5);",
            ],
            {"table": table_source},
        )

        runtime.execute(task.run)
        assert get_rows(conn, table) == [(2015, 4), (2015, 10)]
        runtime.execute(task.output.delete)
        assert not table_exists(conn.sqlalchemy, table)


def test_map_table_resumable(app: App):
    def failing(val: int):
        yield {"doubled": val * 2}
        if val >= 10:
            raise IntentionallyFailError()

    table_source = Table("source_args")
    table = Table("test_map_table_resumable")

    with app.init_context() as init:
        task = MapToNewTable(
            source_table=table_source,
            select="SELECT id, val FROM {{source}} WHERE NOT {{is_done}} ORDER BY id",
            table=table,
            columns=[ValueColumn("doubled", "INT")],
            is_done_column="__success",
            fn=compose(failing, pop_id_fields("id")),
        ).create(init)

    with app.runtime_context() as runtime:
        runtime.get(ConnectionEnvironment).render_executescript(
            [
                """\
                CREATE TABLE {{table}}(
                    id {{ utils.autoincrement_primary_key() }},
                    val INT
                );""",
                "INSERT INTO {{table}}(val) VALUES (2),(5),(12);",
            ],
            {"table": table_source},
        )

        with pytest.raises(IntentionallyFailError):
            runtime.execute(task.run)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)

        assert get_rows(conn, table) == [(4,), (10,)]
        assert get_rows(conn, table_source, order_by=["id"]) == [
            (1, 2, True),
            (2, 5, True),
            (3, 12, False),
        ]
        runtime.execute(task.output.delete)
        assert not table_exists(conn.sqlalchemy, table)


def test_map_table_continue(app: App):
    table_source = Table("test_continue_source")
    table_dest = Table("test_continue_dest")

    def double(num: int):
        yield {"doubled": num * 2}

    with app.init_context() as init:
        task = MapToNewTable(
            source_table=table_source,
            select="SELECT num FROM {{source}} WHERE NOT {{is_done}}",
            table=table_dest,
            columns=[ValueColumn("doubled", "INT")],
            is_done_column="__done",
            fn=compose(double, pop_id_fields("num", keep=True)),
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        conn.render_executescript(
            """\
            CREATE TABLE {{source}}(
                num INT PRIMARY KEY,
                __done BOOL
            );
            {%-split-%}
            INSERT INTO {{source}} VALUES
            (2, TRUE),
            (3, FALSE);
            {%-split-%}
            CREATE TABLE {{dest}}(
                doubled INT
            );
            {%-split-%}
            INSERT INTO {{dest}} VALUES (4);""",
            {"source": table_source, "dest": table_dest},
        )

        assert not runtime.execute(task.output.exists)
        runtime.execute(task.run)
        assert runtime.execute(task.output.exists)
        assert get_rows(conn, table_source) == [(2, True), (3, True)]
        assert get_rows(conn, table_dest) == [(4,), (6,)]
