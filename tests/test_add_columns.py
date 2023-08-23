from ralsei import Table, CreateTableSql, AddColumnsSql, Column
from common.db_helper import get_rows
from ralsei.connection import PsycopgConn
from ralsei.renderer import RalseiRenderer


def prepare_table_task(table: Table):
    task = CreateTableSql(
        sql="""\
        CREATE TABLE {{ table }}(
            a INT
        );
        INSERT INTO {{ table }} VALUES (2), (5);""",
        table=table,
    )
    task.render(RalseiRenderer())
    return task


def test_add_column(conn: PsycopgConn):
    table = Table("test_add_column")
    prepare_table_task(table).run(conn)

    task = AddColumnsSql(
        sql="""\
        UPDATE {{ table }} SET b = a * 2;
        UPDATE {{ table }} SET c = a || '-' || b;""",
        table=table,
        columns=[Column("b", "INT"), Column("c", "TEXT")],
    )
    task.render(RalseiRenderer())

    task.run(conn)
    assert get_rows(conn, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(conn)
    assert get_rows(conn, table) == [(2,), (5,)]


def test_add_column_jinja_var(conn: PsycopgConn):
    table = Table("test_add_column")
    prepare_table_task(table).run(conn)

    task = AddColumnsSql(
        sql="""\
        {% set columns = [
            Column("b", "INT"),
            Column("c", "TEXT")
        ] %}

        UPDATE {{ table }} SET b = a * 2;
        UPDATE {{ table }} SET c = a || '-' || b;""",
        table=table,
    )
    task.render(RalseiRenderer())

    task.run(conn)
    assert get_rows(conn, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(conn)
    assert get_rows(conn, table) == [(2,), (5,)]


def test_column_template(conn: PsycopgConn):
    table = Table("test_add_column")
    prepare_table_task(table).run(conn)

    task = AddColumnsSql(
        sql="",
        table=table,
        columns=[Column("b", "TEXT DEFAULT {{ default }}")],
        params={"default": "Hello"},
    )
    task.render(RalseiRenderer())

    task.run(conn)
    assert get_rows(conn, table) == [(2, "Hello"), (5, "Hello")]
    task.delete(conn)
    assert get_rows(conn, table) == [(2,), (5,)]
