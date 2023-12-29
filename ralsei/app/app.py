from typing import Annotated, Callable
import typer


from ralsei.pipeline import Pipeline
from ralsei.context import EngineContext
from .typer_merge import TyperMerge


class Ralsei:
    def __init__(self, pipeline_constructor: Callable[..., Pipeline]) -> None:
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
