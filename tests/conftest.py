import sys
from pathlib import Path

import pytest
import psycopg

# Helper module used in multiple tests
sys.path.append(Path(__file__).parent.joinpath("common").__str__())


@pytest.fixture
def conn():
    conn = psycopg.connect("dbname=ralsei_test", autocommit=False)
    yield conn
    conn.close()
