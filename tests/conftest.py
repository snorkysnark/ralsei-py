from pathlib import Path
import sys
from pytest import fixture, FixtureRequest

from ralsei.context import Context


# Helper module used in multiple tests
sys.path.append(str(Path(__file__).parent.joinpath("common")))


@fixture(params=["postgresql:///ralsei_test", "sqlite:///ralsei_test.sqlite"])
def ctx(request: FixtureRequest):
    with Context(request.param) as ctx:
        yield ctx
