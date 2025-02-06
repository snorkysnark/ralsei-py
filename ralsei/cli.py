import sys
import subprocess
from typing import Callable
import click
from rich.console import Console
import typer

from ralsei.injector import DIContext
from ralsei.task import Task, ImplTask
from ralsei.viz import VisualGraph
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE


def _constructor_to_click_command(root_constructor: Callable[..., Task]):
    typer_app = typer.Typer(add_help_option=False, add_completion=False)
    typer_app.command()(root_constructor)

    return typer.main.get_command(typer_app)


def _open_in_default_app(filename: str):
    if sys.platform == "win32":
        os.startfile(filename)
    elif sys.platform == "darwin":
        subprocess.Popen(["open", filename])
    elif sys.platform == "linux":
        subprocess.Popen(["xdg-open", filename])


def _ctx_find_task(ctx: click.Context):
    if root := ctx.find_object(ImplTask):
        return root
    raise RuntimeError("click context not set")


def _build_subcommand(group: click.Group, name: str):
    @group.command(name)
    @click.pass_context
    def cmd(ctx: click.Context):
        root = _ctx_find_task(ctx)
        getattr(root, name)(DIContext())


def build_cli(root_constructor: Callable[..., Task]):
    constructor_cmd = _constructor_to_click_command(root_constructor)

    @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        root: Task = constructor_cmd.callback(
            **kwargs
        )  # pyright: ignore[reportOptionalCall]
        root_impl = root.create(DIContext())

        ctx.obj = root_impl

    for param in constructor_cmd.params:
        if not isinstance(param, click.Option):
            raise ValueError(
                f"{param.human_readable_name} is a positional argument. Only typer.Option arguments are allowed!"
            )

        cli.params.append(param)

    _build_subcommand(cli, "run")
    _build_subcommand(cli, "delete")
    _build_subcommand(cli, "redo")

    @cli.command("graph")
    @click.pass_context
    def graph_cmd(ctx: click.Context):
        root = _ctx_find_task(ctx)
        _open_in_default_app(VisualGraph(root).build().render(format="png"))

    return cli


error_console = Console(stderr=True)


def run_cli(root_constructor: Callable[..., Task], *args, **kwargs):
    try:
        build_cli(root_constructor)(*args, **kwargs)
    except Exception as err:
        if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
            error_console.print("Row context:", row_context)
        raise
