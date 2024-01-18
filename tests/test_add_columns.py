from ralsei import JinjaSqlConnection, Table, AddColumnsSql, Column

from common.db_helper import get_rows


def create_table(jsql: JinjaSqlConnection, table: Table):
    jsql.render_executescript(
        [
            """\
            CREATE TABLE {{ table }}(
                a INT
            );""",
            "INSERT INTO {{ table }} VALUES (2), (5);",
        ],
        {"table": table},
    )


def test_add_columns(jsql: JinjaSqlConnection):
    table = Table("test_add_column")
    create_table(jsql, table)

    task = AddColumnsSql(
        sql=[
            "UPDATE {{ table }} SET b = a * 2;",
            "UPDATE {{ table }} SET c = a || '-' || b;",
        ],
        table=table,
        columns=[Column("b", "INT"), Column("c", "TEXT")],
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(jsql)
    assert get_rows(jsql, table) == [(2,), (5,)]


def test_add_columns_jinja_var(jsql: JinjaSqlConnection):
    table = Table("test_add_column")
    create_table(jsql, table)

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
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(jsql)
    assert get_rows(jsql, table) == [(2,), (5,)]


def test_column_template(jsql: JinjaSqlConnection):
    table = Table("test_add_column")
    create_table(jsql, table)

    task = AddColumnsSql(
        sql="",
        table=table,
        columns=[Column("b", "TEXT DEFAULT {{ default }}")],
        params={"default": "Hello"},
    ).create(jsql.jinja)

    task.run(jsql)
    assert get_rows(jsql, table) == [(2, "Hello"), (5, "Hello")]
    task.delete(jsql)
    assert get_rows(jsql, table) == [(2,), (5,)]
