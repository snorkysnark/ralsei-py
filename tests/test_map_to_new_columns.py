from psycopg.sql import Identifier
import pytest

from ralsei import (
    Table,
    MapToNewColumns,
    ValueColumn,
    FnBuilder,
)
from ralsei.connection import PsycopgConn
from sqlalchemy import Engine

from common.db_helper import get_rows


def test_map_columns(conn: PsycopgConn):
    def double(val: int):
        return {"doubled": val * 2}

    table = Table("test_map_columns")
    conn.execute_template(
        """\
        CREATE TABLE {{table}}(
            id SERIAL PRIMARY KEY,
            val INT
        );
        INSERT INTO {{table}}(val) VALUES
        (2),(5),(12);""",
        {"table": table},
    )

    task = MapToNewColumns(
        table=table,
        select="SELECT id, val FROM {{table}}",
        columns=[ValueColumn("doubled", "INT")],
        fn=FnBuilder(double).pop_id_fields("id"),
    )

    task.run(conn)
    assert get_rows(conn, table, order_by=[Identifier("id")]) == [
        (1, 2, 4),
        (2, 5, 10),
        (3, 12, 24),
    ]
    task.delete(conn)
    assert get_rows(conn, table, order_by=[Identifier("id")]) == [
        (1, 2),
        (2, 5),
        (3, 12),
    ]


def test_map_columns_resumable(engine: Engine):
    def failing(val: int):
        if val < 10:
            return {"doubled": val * 2}
        else:
            raise RuntimeError()

    table = Table("test_map_columns_resumable")
    with PsycopgConn(engine.connect()) as conn:
        conn.execute_template(
            """\
            CREATE TABLE {{table}}(
                id SERIAL PRIMARY KEY,
                val INT
            );
            INSERT INTO {{table}}(val) VALUES
            (2),(5),(12);""",
            {"table": table},
        )

        task = MapToNewColumns(
            table=table,
            select="SELECT id, val FROM {{table}}",
            columns=[ValueColumn("doubled", "INT")],
            fn=FnBuilder(failing).pop_id_fields("id"),
            is_done_column="__success",
        )

        with pytest.raises(RuntimeError):
            task.run(conn)

    with PsycopgConn(engine.connect()) as conn:
        assert get_rows(conn, table, order_by=[Identifier("id")]) == [
            (1, 2, 4, True),
            (2, 5, 10, True),
            (3, 12, None, False),
        ]
        task.delete(conn)
        assert get_rows(conn, table, order_by=[Identifier("id")]) == [
            (1, 2),
            (2, 5),
            (3, 12),
        ]
