from typing import Callable, Sequence
import click
from rich.console import Console
from typer import Typer
import typer.main

from ralsei.app import Ralsei
from ralsei.graph import TaskSequence
from ralsei.graph import TreePath
from ralsei.injector import DIContainer
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE

from .click_types import type_treepath

error_console = Console(stderr=True)


def _build_subcommand(
    group: click.Group,
    name: str,
    action: Callable[[TaskSequence, DIContainer], None],
):
    @click.option(
        "--from",
        "from_filters",
        help="run this task and its descendants",
        type=type_treepath,
        multiple=True,
    )
    @click.option(
        "--one",
        "single_filters",
        help="run only this task",
        type=type_treepath,
        multiple=True,
    )
    @group.command(name)
    @click.pass_context
    def cmd(
        ctx: click.Context,
        from_filters: Sequence[TreePath],
        single_filters: Sequence[TreePath],
    ):
        app = ctx.find_object(Ralsei)
        if not app:
            raise RuntimeError("click context not set")

        sequence = app.dag.sort_filtered(from_filters, single_filters)
        with app.runtime() as runtime:
            action(sequence, runtime)


def build_cli(app_constructor: Callable[..., Ralsei]) -> click.Group:
    typer_app = Typer(add_completion=False, add_help_option=False)
    typer_app.command()(app_constructor)

    app_constructor_click = typer.main.get_command(typer_app)

    @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        # Pass custom arguments to app constructor
        ctx.obj = app_constructor_click.callback(**kwargs)  # type:ignore

    for param in app_constructor_click.params:
        if not isinstance(param, click.Option):
            raise ValueError(
                f"{param.human_readable_name} is a positional argument. Only typer.Option arguments are allowed!"
            )

        cli.params.append(param)

    _build_subcommand(cli, "run", TaskSequence.run)
    _build_subcommand(cli, "delete", TaskSequence.delete)
    _build_subcommand(cli, "redo", TaskSequence.redo)

    return cli


def run_cli(app_constructor: Callable[..., Ralsei], *args, **kwargs):
    try:
        build_cli(app_constructor)(*args, **kwargs)
    except Exception as err:
        if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
            error_console.print("Row context:", row_context)
        raise
