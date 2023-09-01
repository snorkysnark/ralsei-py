from psycopg.sql import SQL, Identifier
import pytest

from ralsei import (
    MapToNewTable,
    Table,
    ValueColumn,
    GeneratorBuilder,
)
from ralsei.connection import PsycopgConn
from ralsei.renderer import DEFAULT_RENDERER
from sqlalchemy import Engine

from common.db_helper import get_rows, table_exists


def test_map_new_table_noselect(conn: PsycopgConn):
    def make_rows():
        yield {"foo": 1, "bar": "a"}
        yield {"foo": 2, "bar": "b"}

    table = Table("test_map_table")
    task = MapToNewTable(
        table=table,
        columns=[
            "id SERIAL PRIMARY KEY",
            ValueColumn("foo", "INT"),
            ValueColumn("bar", "TEXT"),
        ],
        fn=make_rows,
    )
    task.render(DEFAULT_RENDERER)

    task.run(conn)
    assert get_rows(conn, table) == [(1, 1, "a"), (2, 2, "b")]
    task.delete(conn)
    assert not table_exists(conn, table)


def test_map_table_jinja(conn: PsycopgConn):
    def double(foo: int):
        yield {"foo": foo * 2}

    table_source = Table("source_args")
    conn.pg.execute(
        DEFAULT_RENDERER.render(
            """\
            CREATE TABLE {{table}}(
                foo INT
            );
            INSERT INTO {{table}} VALUES
            (2),
            (5);""",
            {"table": table_source},
        )
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
        params={"year": 2015, "foo_type": SQL("SMALLINT")},
    )
    task.render(DEFAULT_RENDERER)

    task.run(conn)
    assert get_rows(conn, table) == [(2015, 4), (2015, 10)]
    task.delete(conn)
    assert not table_exists(conn, table)


def test_map_table_resumable(engine: Engine):
    def failing(val: int):
        yield {"doubled": val * 2}
        if val >= 10:
            raise RuntimeError()

    table_source = Table("source_args")

    with PsycopgConn(engine.connect()) as conn:
        conn.pg.execute(
            DEFAULT_RENDERER.render(
                """\
                CREATE TABLE {{table}}(
                    id SERIAL PRIMARY KEY,
                    val INT
                );
                INSERT INTO {{table}}(val) VALUES
                (2),(5),(12);""",
                {"table": table_source},
            )
        )

        table = Table("test_map_table_resumable")
        task = MapToNewTable(
            source_table=table_source,
            select="SELECT id, val FROM {{source}} WHERE NOT {{is_done}} ORDER BY id",
            table=table,
            columns=[ValueColumn("doubled", "INT")],
            is_done_column="__success",
            fn=GeneratorBuilder(failing).pop_id_fields("id"),
        )
        task.render(DEFAULT_RENDERER)

        with pytest.raises(RuntimeError):
            task.run(conn)

    with PsycopgConn(engine.connect()) as conn:
        assert get_rows(conn, table) == [(4,), (10,)]
        assert get_rows(conn, table_source, order_by=[Identifier("id")]) == [
            (1, 2, True),
            (2, 5, True),
            (3, 12, False),
        ]
        task.delete(conn)
        assert not table_exists(conn, table)
