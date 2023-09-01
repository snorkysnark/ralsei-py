import sys
from pathlib import Path

import pytest
import sqlalchemy

from ralsei.connection import PsycopgConn

# Helper module used in multiple tests
sys.path.append(Path(__file__).parent.joinpath("common").__str__())

TEST_DB = "postgresql+psycopg:///ralsei_test"


@pytest.fixture
def engine():
    _engine = sqlalchemy.create_engine(TEST_DB)

    with PsycopgConn(_engine.connect()) as conn:
        conn.pg.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            """
        )
        conn.pg.commit()

    return _engine


@pytest.fixture
def conn():
    engine = sqlalchemy.create_engine(TEST_DB)
    with PsycopgConn(engine.connect()) as conn:
        conn.pg.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            """
        )
        yield conn
