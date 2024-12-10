from ralsei import ConnectionEnvironment, App, Table, AddColumnsSql, Column

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


def test_add_columns(app: App):
    table = Table("test_add_column")

    with app.init_context() as init:
        task = AddColumnsSql(
            sql=[
                "UPDATE {{ table }} SET b = a * 2;",
                "UPDATE {{ table }} SET c = a || '-' || b;",
            ],
            table=table,
            columns=[Column("b", "INT"), Column("c", "TEXT")],
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        create_table(conn, table)

        runtime.execute(task.run)
        assert get_rows(conn, table) == [
            (2, 4, "2-4"),
            (5, 10, "5-10"),
        ]
        runtime.execute(task.output.delete)
        assert get_rows(conn, table) == [(2,), (5,)]


def test_add_columns_jinja_var(app: App):
    table = Table("test_add_column")

    with app.init_context() as init:
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
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        create_table(conn, table)

        runtime.execute(task.run)
        assert get_rows(conn, table) == [
            (2, 4, "2-4"),
            (5, 10, "5-10"),
        ]
        runtime.execute(task.output.delete)
        assert get_rows(conn, table) == [(2,), (5,)]


def test_column_template(app: App):
    table = Table("test_add_column")

    with app.init_context() as init:
        task = AddColumnsSql(
            sql="",
            table=table,
            columns=[Column("b", "TEXT DEFAULT {{ default }}")],
            params={"default": "Hello"},
        ).create(init)

    with app.runtime_context() as runtime:
        conn = runtime.get(ConnectionEnvironment)
        create_table(conn, table)

        runtime.execute(task.run)
        assert get_rows(conn, table) == [(2, "Hello"), (5, "Hello")]
        runtime.execute(task.output.delete)
        assert get_rows(conn, table) == [(2,), (5,)]
