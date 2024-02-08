import sys
from pathlib import Path
from pytest import fixture, FixtureRequest

from ralsei.connection import SqlEngine


# Helper module used in multiple tests
sys.path.append(str(Path(__file__).parent.joinpath("common")))

DATABASE_URLS = ["postgresql:///ralsei_test", "sqlite:///ralsei_test.sqlite"]


def postgres_engine():
    engine = SqlEngine.create("postgresql:///ralsei_test")
    with engine.connect() as conn:
        conn.sqlalchemy.execute_text("DROP SCHEMA public CASCADE;")
        conn.sqlalchemy.execute_text("CREATE SCHEMA public;")
        conn.sqlalchemy.commit()

    return engine


def sqlite_engine():
    Path("ralsei_test.sqlite").unlink(missing_ok=True)

    return SqlEngine.create("sqlite:///ralsei_test.sqlite")


@fixture(params=[postgres_engine, sqlite_engine])
def engine(request: FixtureRequest):
    return request.param()


@fixture()
def conn(engine: SqlEngine):
    with engine.connect() as conn:
        yield conn
