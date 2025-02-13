import os
import sys
import subprocess
from typing import Callable, Optional
import click
from rich.console import Console
import typer

from ralsei.task import Task, Settled
from ralsei.viz import VisualGraph, GraphSettings
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE

from .click_types import TYPE_TASKPATH


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
    if root := ctx.find_object(Settled):
        return root
    raise RuntimeError("click context not set")


def _transform_node(node: Settled, path: list[str], start_from: Optional[str] = None):
    for step in path:
        node = node.navigate(step)
    if start_from:
        node = node.mask(start_from)

    return node


def _build_subcommand(
    group: click.Group,
    name: str,
    get_method: Callable[[Settled], Callable[[], None]],
):
    @click.argument("path", type=TYPE_TASKPATH, required=False, default=[])
    @click.option("--from", "start_from", help="Only this task and its descendants")
    @group.command(name)
    @click.pass_context
    def cmd(ctx: click.Context, path: list[str], start_from: Optional[str] = None):
        task = _transform_node(_ctx_find_task(ctx), path, start_from)
        get_method(task)()


def build_cli(root_constructor: Callable[..., Task]):
    constructor_cmd = _constructor_to_click_command(root_constructor)

    @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        root: Task = constructor_cmd.callback(
            **kwargs
        )  # pyright: ignore[reportOptionalCall]
        root_impl = Settled(root)

        ctx.obj = root_impl

    for param in constructor_cmd.params:
        if not isinstance(param, click.Option):
            raise ValueError(
                f"{param.human_readable_name} is a positional argument. Only typer.Option arguments are allowed!"
            )

        cli.params.append(param)

    _build_subcommand(cli, "run", lambda task: task.run)
    _build_subcommand(cli, "delete", lambda task: task.delete)
    _build_subcommand(cli, "redo", lambda task: task.redo)

    @click.argument("path", type=TYPE_TASKPATH, required=False, default=[])
    @click.option("--from", "start_from", help="Only this task and its descendants")
    @click.option("--max-depth", type=int, help="Limit graph depth")
    @cli.command("graph")
    @click.pass_context
    def graph_cmd(
        ctx: click.Context,
        path: list[str],
        start_from: Optional[str] = None,
        max_depth: Optional[int] = None,
    ):
        task = _transform_node(_ctx_find_task(ctx), path, start_from)

        settings = GraphSettings()
        if max_depth is not None:
            settings.max_depth = len(path) + max_depth

        _open_in_default_app(VisualGraph(task, settings).build().render(format="png"))

    return cli


error_console = Console(stderr=True)


def run_cli(root_constructor: Callable[..., Task], *args, **kwargs):
    try:
        build_cli(root_constructor)(*args, **kwargs)
    except Exception as err:
        if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
            error_console.print("Row context:", row_context)
        raise
