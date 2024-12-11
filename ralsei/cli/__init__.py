import sys
import subprocess
from typing import Callable, NamedTuple, Sequence
import click
from rich.console import Console

from ralsei.app import App
from ralsei.graph import TaskSequence, TaskName, Pipeline
from ralsei.injector import DIContainer
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE
from ralsei.utils import expect

from .click_types import type_taskname
from .reflection import constructor_to_click_command

error_console = Console(stderr=True)


class PipelineContext(NamedTuple):
    app: App
    pipeline: Pipeline


def _build_subcommand(
    group: click.Group,
    name: str,
    action: Callable[[TaskSequence, DIContainer], None],
):
    @click.option(
        "--from",
        "from_filters",
        help="run this task and its descendants",
        type=type_taskname,
        multiple=True,
    )
    @click.option(
        "--one",
        "single_filters",
        help="run only this task",
        type=type_taskname,
        multiple=True,
    )
    @group.command(name)
    @click.pass_context
    def cmd(
        ctx: click.Context,
        from_filters: Sequence[TaskName],
        single_filters: Sequence[TaskName],
    ):
        app, pipeline = expect(
            ctx.find_object(PipelineContext), RuntimeError("click context not set")
        )

        with app.init_context() as init:
            dag = pipeline.build_dag(init)
            sequence = dag.sort_filtered(from_filters, single_filters)
        with app.runtime_context() as runtime:
            action(sequence, runtime)


def open_in_default_app(filename: str):
    if sys.platform == "win32":
        os.startfile(filename)
    elif sys.platform == "darwin":
        subprocess.Popen(["open", filename])
    elif sys.platform == "linux":
        subprocess.Popen(["xdg-open", filename])


def build_cli(pipeline_constructor: Callable[..., Pipeline]) -> click.Group:
    constructor_cmd, app_param_name = constructor_to_click_command(pipeline_constructor)

    @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        app = App()
        if app_param_name:
            kwargs[app_param_name] = app

        # Pass custom arguments to pipeline constructor
        pipeline = constructor_cmd.callback(
            **kwargs
        )  # pyright: ignore[reportOptionalCall]

        ctx.obj = PipelineContext(app, pipeline)

    for param in constructor_cmd.params:
        if not isinstance(param, click.Option):
            raise ValueError(
                f"{param.human_readable_name} is a positional argument. Only typer.Option arguments are allowed!"
            )

        cli.params.append(param)

    _build_subcommand(cli, "run", TaskSequence.run)
    _build_subcommand(cli, "delete", TaskSequence.delete)
    _build_subcommand(cli, "redo", TaskSequence.redo)

    @click.argument("filename", default="graph.dot")
    @cli.command("graph")
    @click.pass_context
    def graph_cmd(ctx: click.Context, filename: str):
        app, pipeline = expect(
            ctx.find_object(PipelineContext), RuntimeError("click context not set")
        )

        with app.init_context() as init:
            dag = pipeline.build_dag(init)

        image = dag.graphviz().render(filename, format="png")
        open_in_default_app(image)

    return cli


def run_cli(pipeline_constructor: Callable[..., Pipeline], *args, **kwargs):
    try:
        build_cli(pipeline_constructor)(*args, **kwargs)
    except Exception as err:
        if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
            error_console.print("Row context:", row_context)
        raise
