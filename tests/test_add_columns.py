from ralsei import Context, Table, AddColumnsSql, Column

from common.db_helper import get_rows


def create_table(ctx: Context, table: Table):
    ctx.render_executescript(
        [
            """\
            CREATE TABLE {{ table }}(
                a INT
            );""",
            "INSERT INTO {{ table }} VALUES (2), (5);",
        ],
        {"table": table},
    )


def test_add_columns(ctx: Context):
    table = Table("test_add_column")
    create_table(ctx, table)

    task = AddColumnsSql(
        sql=[
            "UPDATE {{ table }} SET b = a * 2;",
            "UPDATE {{ table }} SET c = a || '-' || b;",
        ],
        table=table,
        columns=[Column("b", "INT"), Column("c", "TEXT")],
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(ctx)
    assert get_rows(ctx, table) == [(2,), (5,)]


def test_add_columns_jinja_var(ctx: Context):
    table = Table("test_add_column")
    create_table(ctx, table)

    task = AddColumnsSql(
        sql="""\
        {% set columns = [
            Column("b", "INT"),
            Column("c", "TEXT")
        ] -%}

        UPDATE {{ table }} SET b = a * 2;
        {%-split-%}
        UPDATE {{ table }} SET c = a || '-' || b;""",
        table=table,
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(ctx)
    assert get_rows(ctx, table) == [(2,), (5,)]


def test_column_template(ctx: Context):
    table = Table("test_add_column")
    create_table(ctx, table)

    task = AddColumnsSql(
        sql="",
        table=table,
        columns=[Column("b", "TEXT DEFAULT {{ default }}")],
        params={"default": "Hello"},
    ).create(ctx)

    task.run(ctx)
    assert get_rows(ctx, table) == [(2, "Hello"), (5, "Hello")]
    task.delete(ctx)
    assert get_rows(ctx, table) == [(2,), (5,)]
