import pytest
from ralsei import (
    App,
    ConnectionEnvironment,
    Table,
    MapToNewColumns,
    ValueColumn,
    compose_one,
    pop_id_fields,
)

from tests.db_helper import get_rows
from tests.error import IntentionallyFailError


def test_map_columns(app: App):
    table = Table("test_map_columns")

    def double(val: int):
        return {"doubled": val * 2}

    with app.init_context() as init:
        task = MapToNewColumns(
            table=table,
            select="SELECT id, val FROM {{table}}",
            columns=[ValueColumn("doubled", "INT")],
            fn=compose_one(double, pop_id_fields("id")),
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        conn.render_executescript(
            [
                """\
                CREATE TABLE {{table}}(
                    id {{autoincrement_primary_key}},
                    val INT
                );""",
                "INSERT INTO {{table}}(val) VALUES (2),(5),(12);",
            ],
            {"table": table},
        )

        runtime.execute(task.run)
        assert get_rows(conn, table, order_by=["id"]) == [
            (1, 2, 4),
            (2, 5, 10),
            (3, 12, 24),
        ]
        runtime.execute(task.output.delete)
        assert get_rows(conn, table, order_by=["id"]) == [
            (1, 2),
            (2, 5),
            (3, 12),
        ]


def test_map_columns_resumable(app: App):
    table = Table("test_map_columns_resumable")

    def failing(val: int):
        if val < 10:
            return {"doubled": val * 2}
        else:
            raise IntentionallyFailError()

    with app.init_context() as init:
        task = MapToNewColumns(
            table=table,
            select="SELECT id, val FROM {{table}}",
            columns=[ValueColumn("doubled", "INT")],
            fn=compose_one(failing, pop_id_fields("id")),
            is_done_column="__success",
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        conn.render_executescript(
            [
                """\
                CREATE TABLE {{table}}(
                    id {{autoincrement_primary_key}},
                    val INT
                );""",
                "INSERT INTO {{table}}(val) VALUES (2),(5),(12);",
            ],
            {"table": table},
        )

        with pytest.raises(IntentionallyFailError):
            runtime.execute(task.run)

    with app.runtime_context() as runtime:
        assert get_rows(runtime.get(ConnectionEnvironment), table, order_by=["id"]) == [
            (1, 2, 4, True),
            (2, 5, 10, True),
            (3, 12, None, False),
        ]
        runtime.execute(task.output.delete)
        assert get_rows(runtime.get(ConnectionEnvironment), table, order_by=["id"]) == [
            (1, 2),
            (2, 5),
            (3, 12),
        ]


def test_map_columns_continue(app: App):
    table = Table("resumable")

    def double(num: int):
        return {"doubled": num * 2}

    with app.init_context() as init:
        task = MapToNewColumns(
            table=table,
            select="SELECT num FROM {{table}} WHERE NOT {{is_done}}",
            columns=[ValueColumn("doubled", "INT")],
            is_done_column="__done",
            fn=compose_one(double, pop_id_fields("num", keep=True)),
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
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

        assert not runtime.execute(task.output.exists)
        runtime.execute(task.run)
        assert runtime.execute(task.output.exists)
        assert get_rows(conn, table) == [(2, 4, True), (3, 6, True)]
