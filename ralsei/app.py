import click
from click.utils import _detect_program_name
from rich_click import RichGroup, RichCommand, rich_config, RichHelpConfiguration
from rich import traceback
from typing import Callable, Sequence, overload

from ralsei.pipeline import Pipeline
from ralsei.context import EngineContext
from ralsei.console import console


def extend_params(
    extra_params: Sequence[click.Parameter],
) -> Callable[[click.Command], click.Command]:
    def decorator(cmd: click.Command) -> click.Command:
        cmd.params.extend(extra_params)
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


class Ralsei:
    @overload
    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline],
        custom_cli_options: list[click.Option] = [],
    ) -> None:
        ...

    @overload
    def __init__(self, pipeline_source: Pipeline) -> None:
        ...

    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline] | Pipeline,
        custom_cli_options: list[click.Option] = [],
    ) -> None:
        if isinstance(pipeline_source, Pipeline):
            self._pipeline_constructor = lambda: pipeline_source
            self._custom_cli_options = []
        else:
            self._pipeline_constructor = pipeline_source
            self._custom_cli_options = custom_cli_options

    def run(self):
        @rich_config(
            console=console,
            help_config=RichHelpConfiguration(
                option_groups=create_option_group_settings(
                    ["run"], self._custom_cli_options
                )
            ),
        )
        @click.group(
            cls=RichGroup, context_settings=dict(help_option_names=["-h", "--help"])
        )
        def cli():
            pass

        @extend_params(self._custom_cli_options)
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @cli.command("run", cls=RichCommand)
        def run_cmd(db: str, *args, **kwargs):
            pipeline = self._pipeline_constructor(*args, **kwargs)
            engine = EngineContext.create(db)
            dag = pipeline.build_dag(engine.jinja)

            with engine.connect() as ctx:
                dag.run(ctx)

        traceback.install(console=console, show_locals=True)
        cli()


__all__ = ["Ralsei"]
