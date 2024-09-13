import pytest
from ralsei import (
    ConnectionEnvironment,
    Table,
    MapToNewColumns,
    ValueColumn,
    compose_one,
    pop_id_fields,
)
import sqlalchemy

from tests.db_helper import get_rows


def test_map_columns(conn: ConnectionEnvironment):
    def double(val: int):
        return {"doubled": val * 2}

    table = Table("test_map_columns")
    conn.render_executescript(
        [
            """\
            CREATE TABLE {{table}}(
                id {{dialect.autoincrement_key}},
                val INT
            );""",
            "INSERT INTO {{table}}(val) VALUES (2),(5),(12);",
        ],
        {"table": table},
    )

    task = MapToNewColumns(
        table=table,
        select="SELECT id, val FROM {{table}}",
        columns=[ValueColumn("doubled", "INT")],
        fn=compose_one(double, pop_id_fields("id")),
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table, order_by=["id"]) == [
        (1, 2, 4),
        (2, 5, 10),
        (3, 12, 24),
    ]
    task.delete(conn.sqlalchemy)
    assert get_rows(conn, table, order_by=["id"]) == [
        (1, 2),
        (2, 5),
        (3, 12),
    ]


def test_map_columns_resumable(engine: sqlalchemy.Engine):
    def failing(val: int):
        if val < 10:
            return {"doubled": val * 2}
        else:
            raise RuntimeError()

    table = Table("test_map_columns_resumable")
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
            {"table": table},
        )

        task = MapToNewColumns(
            table=table,
            select="SELECT id, val FROM {{table}}",
            columns=[ValueColumn("doubled", "INT")],
            fn=compose_one(failing, pop_id_fields("id")),
            is_done_column="__success",
        ).create(conn.jinja.base)

        with pytest.raises(RuntimeError):
            task.run(conn.sqlalchemy)

    with ConnectionEnvironment(engine) as conn:
        assert get_rows(conn, table, order_by=["id"]) == [
            (1, 2, 4, True),
            (2, 5, 10, True),
            (3, 12, None, False),
        ]
        task.delete(conn.sqlalchemy)
        assert get_rows(conn, table, order_by=["id"]) == [
            (1, 2),
            (2, 5),
            (3, 12),
        ]


def test_map_columns_continue(conn: ConnectionEnvironment):
    table = Table("resumable")
    conn.render_executescript(
        """\
        CREATE TABLE {{table}}(
            num INT PRIMARY KEY,
            doubled INT,
            __done BOOL
        );
        {%-split-%}
        INSERT INTO {{table}} VALUES
        (2, 4, TRUE),
        (3, NULL, FALSE);""",
        {"table": table},
    )

    def double(num: int):
        return {"doubled": num * 2}

    task = MapToNewColumns(
        table=table,
        select="SELECT num FROM {{table}} WHERE NOT {{is_done}}",
        columns=[ValueColumn("doubled", "INT")],
        is_done_column="__done",
        fn=compose_one(double, pop_id_fields("num", keep=True)),
    ).create(conn.jinja.base)

    assert not task.exists(conn.sqlalchemy)
    task.run(conn.sqlalchemy)
    assert task.exists(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2, 4, True), (3, 6, True)]
