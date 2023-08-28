import sys
from pathlib import Path

import pytest
import sqlalchemy

from ralsei.connection import PsycopgConn

# Helper module used in multiple tests
sys.path.append(Path(__file__).parent.joinpath("common").__str__())


@pytest.fixture
def conn():
    engine = sqlalchemy.create_engine("postgresql+psycopg:///ralsei_test")
    conn = PsycopgConn(engine.connect())
    conn.pg().execute(
        """
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        """
    )
    yield conn
    conn.sqlalchemy().close()
