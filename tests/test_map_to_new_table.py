import pytest
from ralsei import (
    ConnectionContext,
    EngineContext,
    Table,
    MapToNewTable,
    ValueColumn,
    Sql,
    compose,
    pop_id_fields,
)
from ralsei.db_actions import table_exists

from common.db_helper import get_rows


def test_map_new_table_noselect(ctx: ConnectionContext):
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
    ).create(ctx.jinja)

    task.run(ctx)
    assert get_rows(ctx, table) == [(1, 1, "a"), (2, 2, "b")]
    task.delete(ctx)
    assert not table_exists(ctx, table)


def test_map_table_jinja(ctx: ConnectionContext):
    def double(foo: int):
        yield {"foo": foo * 2}

    table_source = Table("source_args")
    ctx.render_executescript(
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
        params={"year": 2015, "foo_type": Sql("INT")},
    ).create(ctx.jinja)

    task.run(ctx)
    assert get_rows(ctx, table) == [(2015, 4), (2015, 10)]
    task.delete(ctx)
    assert not table_exists(ctx, table)


def test_map_table_resumable(engine: EngineContext):
    def failing(val: int):
        yield {"doubled": val * 2}
        if val >= 10:
            raise RuntimeError()

    table_source = Table("source_args")

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
        ).create(ctx.jinja)

        with pytest.raises(RuntimeError):
            task.run(ctx)

    with engine.connect() as ctx:
        assert get_rows(ctx, table) == [(4,), (10,)]
        assert get_rows(ctx, table_source, order_by=["id"]) == [
            (1, 2, True),
            (2, 5, True),
            (3, 12, False),
        ]
        task.delete(ctx)
        assert not table_exists(ctx, table)
