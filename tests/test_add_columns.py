from ralsei import ConnectionEnvironment, Table, AddColumnsSql, Column

from tests.db_helper import get_rows


def create_table(conn: ConnectionEnvironment, table: Table):
    conn.render_executescript(
        [
            """\
            CREATE TABLE {{ table }}(
                a INT
            );""",
            "INSERT INTO {{ table }} VALUES (2), (5);",
        ],
        {"table": table},
    )


def test_add_columns(conn: ConnectionEnvironment):
    table = Table("test_add_column")
    create_table(conn, table)

    task = AddColumnsSql(
        sql=[
            "UPDATE {{ table }} SET b = a * 2;",
            "UPDATE {{ table }} SET c = a || '-' || b;",
        ],
        table=table,
        columns=[Column("b", "INT"), Column("c", "TEXT")],
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2,), (5,)]


def test_add_columns_jinja_var(conn: ConnectionEnvironment):
    table = Table("test_add_column")
    create_table(conn, table)

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
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2,), (5,)]


def test_column_template(conn: ConnectionEnvironment):
    table = Table("test_add_column")
    create_table(conn, table)

    task = AddColumnsSql(
        sql="",
        table=table,
        columns=[Column("b", "TEXT DEFAULT {{ default }}")],
        locals={"default": "Hello"},
    ).create(conn.jinja.base)

    task.run(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2, "Hello"), (5, "Hello")]
    task.delete(conn.sqlalchemy)
    assert get_rows(conn, table) == [(2,), (5,)]
