import sys
from pathlib import Path
from pytest import fixture, FixtureRequest

from ralsei.jinjasql import JinjaSqlEngine


# Helper module used in multiple tests
sys.path.append(str(Path(__file__).parent.joinpath("common")))

DATABASE_URLS = ["postgresql:///ralsei_test", "sqlite:///ralsei_test.sqlite"]


def postgres_engine():
    engine_ctx = JinjaSqlEngine.create("postgresql:///ralsei_test")
    with engine_ctx.connect().connection as conn:
        conn.execute_text("DROP SCHEMA public CASCADE;")
        conn.execute_text("CREATE SCHEMA public;")
        conn.commit()

    return engine_ctx


def sqlite_engine():
    Path("ralsei_test.sqlite").unlink(missing_ok=True)

    return JinjaSqlEngine.create("sqlite:///ralsei_test.sqlite")


@fixture(params=[postgres_engine, sqlite_engine])
def jengine(request: FixtureRequest):
    return request.param()


@fixture()
def jsql(jengine: JinjaSqlEngine):
    with jengine.connect() as jsql:
        yield jsql
