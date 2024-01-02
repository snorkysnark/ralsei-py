import click
import typer
from click.utils import _detect_program_name
from rich_click import RichGroup, RichCommand, rich_config, RichHelpConfiguration
from rich import traceback
from typing import Any, Callable, Self, Sequence, cast, overload
import itertools

from ralsei.pipeline import Pipeline
from ralsei.context import EngineContext


POSITONAL_ARGS_ERROR = """\
All pipeline constructor args must be annotated as Typer CLI Options.
See: https://typer.tiangolo.com/tutorial/options/help/"""


def get_existing_names(params: Sequence[click.Parameter]) -> tuple[set[str], set[str]]:
    existing_names = set()
    existing_opts = set()
    for param in params:
        if param.name:
            existing_names.add(param.name)
        for opt in param.opts:
            existing_opts.add(opt)
        for opt in param.secondary_opts:
            existing_opts.add(opt)

    return existing_names, existing_opts


def extend_params(
    extra_params: Sequence[click.Parameter],
) -> Callable[[click.Command], click.Command]:
    def decorator(cmd: click.Command) -> click.Command:
        existing_names, existing_opts = get_existing_names(cmd.params)
        for param in extra_params:
            if param.name and param.name in existing_names:
                raise ValueError(f"parameter name {param.name} already occupied")
            for opt in itertools.chain(param.opts, param.secondary_opts):
                if opt in existing_opts:
                    raise ValueError(f"option name {opt} already occupied")

            cmd.params.append(param)

        return cmd

    return decorator


def create_option_group_settings(
    command_names: list[str], custom_options: Sequence[click.Parameter]
):
    prog_name = _detect_program_name()

    return {
        f"{prog_name} {cmd}": [
            {
                "name": "Custom options",
                "options": [param.opts[0] for param in custom_options if param.opts],
            }
        ]
        for cmd in command_names
    }


def build_cli(
    pipeline_constructor: Callable[..., Pipeline],
    custom_options: Sequence[click.Option],
) -> click.Group:
    @rich_config(
        help_config=RichHelpConfiguration(
            option_groups=create_option_group_settings(["run"], custom_options)
        ),
    )
    @click.group(
        cls=RichGroup, context_settings=dict(help_option_names=["-h", "--help"])
    )
    def cli():
        pass

    @extend_params(custom_options)
    @click.option("--db", "-d", help="sqlalchemy database url", required=True)
    @cli.command("run", cls=RichCommand)
    def run_cmd(db: str, *args, **kwargs):
        pipeline = pipeline_constructor(*args, **kwargs)
        engine = EngineContext.create(db)
        dag = pipeline.build_dag(engine.jinja)

        with engine.connect() as ctx:
            dag.run(ctx)

    return cli


class Ralsei:
    @overload
    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline],
        custom_cli_options: Sequence[click.Option] = [],
    ) -> None:
        ...

    @overload
    def __init__(self, pipeline_source: Pipeline) -> None:
        ...

    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline] | Pipeline,
        custom_cli_options: Sequence[click.Option] = [],
    ) -> None:
        if isinstance(pipeline_source, Pipeline):
            self._pipeline_constructor = lambda: pipeline_source
            self._custom_cli_options = []
        else:
            self._pipeline_constructor = pipeline_source
            self._custom_cli_options = custom_cli_options

        self.cli = build_cli(self._pipeline_constructor, self._custom_cli_options)

    @classmethod
    def from_typer(cls, typer_callable: Callable[..., Any]) -> Self:
        typer_app = typer.Typer(add_help_option=False, add_completion=False)
        typer_app.command()(typer_callable)
        command = typer.main.get_command(typer_app)

        assert command.callback, "Generated command is missing a callback"
        for param in command.params:
            if not isinstance(param, click.Option):
                raise ValueError(POSITONAL_ARGS_ERROR)

        return cls(command.callback, cast(Sequence[click.Option], command.params))

    def run(self):
        traceback.install(show_locals=True)
        self.cli()


__all__ = ["Ralsei"]
