import pytest
from ralsei import (
    ConnectionContext,
    EngineContext,
    Table,
    MapToNewColumns,
    ValueColumn,
    compose_one,
    pop_id_fields,
)
from ralsei.task import ExistsStatus

from common.db_helper import get_rows


def test_map_columns(ctx: ConnectionContext):
    def double(val: int):
        return {"doubled": val * 2}

    table = Table("test_map_columns")
    ctx.render_executescript(
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
    ).create(ctx.jinja)

    task.run(ctx)
    assert get_rows(ctx, table, order_by=["id"]) == [
        (1, 2, 4),
        (2, 5, 10),
        (3, 12, 24),
    ]
    task.delete(ctx)
    assert get_rows(ctx, table, order_by=["id"]) == [
        (1, 2),
        (2, 5),
        (3, 12),
    ]


def test_map_columns_resumable(engine: EngineContext):
    def failing(val: int):
        if val < 10:
            return {"doubled": val * 2}
        else:
            raise RuntimeError()

    table = Table("test_map_columns_resumable")
    with engine.connect() as ctx:
        ctx.render_executescript(
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
        ).create(ctx.jinja)

        with pytest.raises(RuntimeError):
            task.run(ctx)

    with engine.connect() as ctx:
        assert get_rows(ctx, table, order_by=["id"]) == [
            (1, 2, 4, True),
            (2, 5, 10, True),
            (3, 12, None, False),
        ]
        task.delete(ctx)
        assert get_rows(ctx, table, order_by=["id"]) == [
            (1, 2),
            (2, 5),
            (3, 12),
        ]


def test_map_columns_continue(ctx: ConnectionContext):
    table = Table("resumable")
    ctx.render_executescript(
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
    ).create(ctx.jinja)

    assert task.exists(ctx) == ExistsStatus.PARTIAL
    task.run(ctx)
    assert task.exists(ctx) == ExistsStatus.YES
    assert get_rows(ctx, table) == [(2, 4, True), (3, 6, True)]
