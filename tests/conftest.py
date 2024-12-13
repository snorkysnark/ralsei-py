import os
from pathlib import Path
from sqlalchemy import text
from pytest import fixture, FixtureRequest

from ralsei.plugin.sql import SqlPlugin
from ralsei.app import App


def sql_plugin_postgres():
    sql = SqlPlugin(os.environ.get("POSTGRES_URL", "postgresql:///ralsei_test"))

    with sql.engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
        conn.commit()

    return sql


def sql_plugin_sqlite():
    Path("ralsei_test.sqlite").unlink(missing_ok=True)

    sql = SqlPlugin("sqlite:///ralsei_test.sqlite")
    return sql


@fixture(params=[sql_plugin_postgres, sql_plugin_sqlite])
def app(request: FixtureRequest):
    return App(plugins=[request.param()])
