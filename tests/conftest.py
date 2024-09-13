import os
from pathlib import Path
from pytest import fixture, FixtureRequest

import sqlalchemy
from ralsei.connection import create_engine, ConnectionExt, ConnectionEnvironment


# Helper module used in multiple tests
# sys.path.append(str(Path(__file__).parent.joinpath("common")))


def postgres_engine():
    db_url = os.environ.get("POSTGRES_URL", "postgresql:///ralsei_test")
    engine = create_engine(db_url)

    with ConnectionExt(engine) as conn:
        conn.execute_text("DROP SCHEMA public CASCADE;")
        conn.execute_text("CREATE SCHEMA public;")
        conn.commit()

    return engine


def sqlite_engine():
    Path("ralsei_test.sqlite").unlink(missing_ok=True)

    return create_engine("sqlite:///ralsei_test.sqlite")


@fixture(params=[postgres_engine, sqlite_engine])
def engine(request: FixtureRequest):
    return request.param()


@fixture()
def conn(engine: sqlalchemy.Engine):
    with ConnectionEnvironment(engine) as conn:
        yield conn
