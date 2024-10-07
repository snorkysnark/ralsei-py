import pytest
from ralsei import (
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


def test_map_new_table_noselect(conn: ConnectionEnvironment):
    def make_rows():
        yield {"foo": 1, "bar": "a"}
        yield {"foo": 2, "bar": "b"}

    table = Table("test_map_table")
    task = MapToNewTable(
        table=table,
        columns=[
            "id {{dialect.autoincrement_key}}",
            ValueColumn("foo", "INT"),
            ValueColumn("bar", "TEXT"),
        ],
        fn=make_rows,
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [(1, 1, "a"), (2, 2, "b")]
    task.delete(conn.sqlalchemy)
    assert not table_exists(conn, table)


def test_map_table_jinja(conn: ConnectionEnvironment):
    def double(foo: int):
        yield {"foo": foo * 2}

    table_source = Table("source_args")
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

    table = Table("test_map_table_select")
    task = MapToNewTable(
        table=table,
        source_table=table_source,
        select="SELECT * FROM {{source}}",
        columns=[
            "year INT DEFAULT {{year}}",
            ValueColumn("foo", "{{foo_type}}"),
        ],
        fn=double,
        locals={"year": 2015, "foo_type": Sql("INT")},
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2015, 4), (2015, 10)]
    task.delete(conn.sqlalchemy)
    assert not table_exists(conn, table)


def test_map_table_resumable(engine: sqlalchemy.Engine):
    def failing(val: int):
        yield {"doubled": val * 2}
        if val >= 10:
            raise RuntimeError()

    table_source = Table("source_args")

    with ConnectionEnvironment(engine) as conn:
        conn.render_executescript(
            [
                """\
                CREATE TABLE {{table}}(
                    id {{dialect.autoincrement_key}},
                    val INT
                );""",
                "INSERT INTO {{table}}(val) VALUES (2),(5),(12);",
            ],
            {"table": table_source},
        )

        table = Table("test_map_table_resumable")
        task = MapToNewTable(
            source_table=table_source,
            select="SELECT id, val FROM {{source}} WHERE NOT {{is_done}} ORDER BY id",
            table=table,
            columns=[ValueColumn("doubled", "INT")],
            is_done_column="__success",
            fn=compose(failing, pop_id_fields("id")),
        ).create(conn.jinja.base)

        with pytest.raises(RuntimeError):
            task.run(conn.sqlalchemy)

    with ConnectionEnvironment(engine) as conn:
        assert get_rows(conn, table) == [(4,), (10,)]
        assert get_rows(conn, table_source, order_by=["id"]) == [
            (1, 2, True),
            (2, 5, True),
            (3, 12, False),
        ]
        task.delete(conn.sqlalchemy)
        assert not table_exists(conn, table)


def test_map_table_continue(conn: ConnectionEnvironment):
    table_source = Table("test_continue_source")
    table_dest = Table("test_continue_dest")
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

    def double(num: int):
        yield {"doubled": num * 2}

    task = MapToNewTable(
        source_table=table_source,
        select="SELECT num FROM {{source}} WHERE NOT {{is_done}}",
        table=table_dest,
        columns=[ValueColumn("doubled", "INT")],
        is_done_column="__done",
        fn=compose(double, pop_id_fields("num", keep=True)),
    ).create(conn.jinja.base)

    assert not task.exists(conn.sqlalchemy)
    task.run(conn.sqlalchemy)
    assert task.exists(conn.sqlalchemy)
    assert get_rows(conn, table_source) == [(2, True), (3, True)]
    assert get_rows(conn, table_dest) == [(4,), (6,)]
