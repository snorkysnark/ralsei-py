from psycopg import Connection

from ralsei import Table, CreateTableSql, AddColumnsSql
from common.db_helper import get_rows


def test_add_column(conn: Connection):
    table = Table("test_create_table")

    CreateTableSql(
        sql="""\
        CREATE TABLE {{ table }}(
            a INT
        );
        INSERT INTO {{ table }} VALUES (2), (5);""",
        table=table,
    ).run(conn)

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

    task.run(conn)
    assert get_rows(conn, table) == [
        (2, 4, "2-4"),
        (5, 10, "5-10"),
    ]
    task.delete(conn)
    assert get_rows(conn, table) == [(2,), (5,)]
