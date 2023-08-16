import sys
from pathlib import Path

import pytest
import sqlalchemy

from ralsei.task.context import MultiConnection

# Helper module used in multiple tests
sys.path.append(Path(__file__).parent.joinpath("common").__str__())


@pytest.fixture
def conn():
    engine = sqlalchemy.create_engine("postgresql+psycopg:///ralsei_test")
    conn = engine.connect()
    yield MultiConnection(conn)
    conn.close()
