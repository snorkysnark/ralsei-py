import sys
import os
from pathlib import Path

import pytest
import sqlalchemy

from ralsei.connection import PsycopgConn, create_connection_url

# Helper module used in multiple tests
sys.path.append(Path(__file__).parent.joinpath("common").__str__())


def create_engine_from_env():
    return sqlalchemy.create_engine(
        create_connection_url(os.environ.get("POSTGRES_URL", "postgres:///ralsei_test"))
    )


@pytest.fixture
def engine():
    _engine = create_engine_from_env()

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
    engine = create_engine_from_env()
    with PsycopgConn(engine.connect()) as conn:
        conn.pg.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            """
        )
        yield conn
