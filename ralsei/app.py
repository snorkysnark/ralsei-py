from typing import Annotated, Callable, get_type_hints
import typer
from typer.models import OptionInfo
from merge_args import merge_args


from .pipeline import Pipeline
from .context import EngineContext


def check_only_optional_args(fn: Callable):
    for arg_type in get_type_hints(fn, include_extras=True):
        if not (
            (metadata := getattr(arg_type, "__metadata__", None))
            and isinstance(metadata, tuple)
            and len(metadata) > 0
            and isinstance(metadata[0], OptionInfo)
        ):
            return False

        return True


class Ralsei:
    def __init__(self, pipeline_constructor: Callable[..., Pipeline]) -> None:
        if not check_only_optional_args(pipeline_constructor):
            raise ValueError(
                """\
All pipeline constructor arguments must be annotated as Typer CLI Options.
See https://typer.tiangolo.com/tutorial/options/help/"""
            )

        self._pipeline_constructor = pipeline_constructor

    def run(self):
        typer_app = typer.Typer(
            add_completion=False,
            context_settings={"help_option_names": ["-h", "--help"]},
        )

        @typer_app.command("run")
        @merge_args(self._pipeline_constructor)
        def run_cmd(db: Annotated[str, typer.Option("--db", "-d")], *args, **kwargs):
            with EngineContext.create(db).connect() as ctx:
                self._pipeline_constructor(*args, **kwargs).build_dag(ctx.jinja).run(
                    ctx
                )

        typer_app()
