from typing import Annotated, Any, Callable
import typer
from typer.utils import get_params_from_function
from typer.models import OptionInfo

from ralsei.pipeline import Pipeline
from ralsei.context import EngineContext
from .typer_merge import TyperMerge

POSITONAL_OPTIONAL_ARG_ERROR = """\
All pipeline constructor args must be annotated as Typer CLI Options.
See: https://typer.tiangolo.com/tutorial/options/help/"""


def check_only_optional_args(fn: Callable[..., Any]) -> bool:
    for arg in get_params_from_function(fn).values():
        if not isinstance(arg.default, OptionInfo):
            return False

    return True


class Ralsei:
    def __init__(
        self, pipeline_constructor: Callable[..., Pipeline] | Pipeline
    ) -> None:
        if isinstance(pipeline_constructor, Pipeline):
            self._pipeline_constructor = lambda: pipeline_constructor
        else:
            if not check_only_optional_args(pipeline_constructor):
                raise ValueError(POSITONAL_OPTIONAL_ARG_ERROR)

            self._pipeline_constructor = pipeline_constructor

    def run(self):
        cli_app = TyperMerge()

        @cli_app.command("run", merge=[self._pipeline_constructor])
        def run_cmd(db: Annotated[str, typer.Option("--db", "-d")], *args, **kwargs):
            pipeline = self._pipeline_constructor(*args, **kwargs)
            engine = EngineContext.create(db)
            dag = pipeline.build_dag(engine.jinja)

            with engine.connect() as ctx:
                dag.run(ctx)

        cli_app()
