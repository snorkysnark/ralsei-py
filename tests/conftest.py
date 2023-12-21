import sys
from pathlib import Path
from sqlalchemy import Engine
from pytest import fixture, FixtureRequest

from ralsei.context import Context, Connection, create_engine


# Helper module used in multiple tests
sys.path.append(str(Path(__file__).parent.joinpath("common")))

DATABASE_URLS = ["postgresql:///ralsei_test", "sqlite:///ralsei_test.sqlite"]


def postgres_engine():
    eng = create_engine("postgresql:///ralsei_test")
    with Connection(eng) as conn:
        conn.execute_text("DROP SCHEMA public CASCADE;")
        conn.execute_text("CREATE SCHEMA public;")
        conn.commit()

    return create_engine("postgresql:///ralsei_test")


def sqlite_engine():
    Path("ralsei_test.sqlite").unlink(missing_ok=True)

    return create_engine("sqlite:///ralsei_test.sqlite")


@fixture(params=[postgres_engine, sqlite_engine])
def engine(request: FixtureRequest):
    return request.param()


@fixture()
def ctx(engine: Engine):
    with Context(engine) as ctx:
        yield ctx
