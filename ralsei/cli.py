import sys
import subprocess
from typing import Callable
import click
from rich.console import Console
import typer

from ralsei.injector import DIContext
from ralsei.task import Task
from ralsei.viz import VisualGraph
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE


def constructor_to_click_command(root_constructor: Callable[..., Task]):
    typer_app = typer.Typer(add_help_option=False, add_completion=False)
    typer_app.command()(root_constructor)

    return typer.main.get_command(typer_app)


def open_in_default_app(filename: str):
    if sys.platform == "win32":
        os.startfile(filename)
    elif sys.platform == "darwin":
        subprocess.Popen(["open", filename])
    elif sys.platform == "linux":
        subprocess.Popen(["xdg-open", filename])


def build_cli(root_constructor: Callable[..., Task]):
    constructor_cmd = constructor_to_click_command(root_constructor)

    @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        root = constructor_cmd.callback(**kwargs)  # pyright: ignore[reportOptionalCall]
        DIContext().initialize_task(root)

        ctx.obj = root

    for param in constructor_cmd.params:
        if not isinstance(param, click.Option):
            raise ValueError(
                f"{param.human_readable_name} is a positional argument. Only typer.Option arguments are allowed!"
            )

        cli.params.append(param)

    @cli.command("graph")
    @click.pass_context
    def graph_cmd(ctx: click.Context):
        root = ctx.find_object(Task)
        if not root:
            raise RuntimeError("click context not set")

        open_in_default_app(VisualGraph(root).build().render(format="png"))

    return cli


error_console = Console(stderr=True)


def run_cli(root_constructor: Callable[..., Task], *args, **kwargs):
    try:
        build_cli(root_constructor)(*args, **kwargs)
    except Exception as err:
        if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
            error_console.print("Row context:", row_context)
        raise
