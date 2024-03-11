from dataclasses import dataclass
from pathlib import Path
import sys
import click
from rich.console import Console
from rich.prompt import Prompt
from typing import Callable, Optional, Sequence, overload

from ralsei.dialect import DEFAULT_DIALECT_REGISTRY
from ralsei.graph import Pipeline, TreePath, TaskSequence, DAG, NamedTask
from ralsei.connection import SqlEngine, SqlConnection
from ralsei.console import console
from ralsei.task import ROW_CONTEXT_ATRRIBUTE

from ._parsers import TYPE_TREEPATH
from ._decorators import extend_params
from ._rich import print_task_info
from ._opener import open_in_default_app

traceback_console = Console(stderr=True)


@dataclass
class GroupContext:
    engine: SqlEngine
    dag: DAG


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
            self._custom_cli_options = [
                *getattr(pipeline_source, "__click_params__", []),
                *custom_cli_options,
            ]

        self.dialect_registry = DEFAULT_DIALECT_REGISTRY.copy()

    def build_cli(self) -> click.Group:
        @extend_params(self._custom_cli_options)
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        @click.pass_context
        def cli(ctx: click.Context, db: str, *args, **kwargs):
            engine = SqlEngine.create(db, dialect_source=self.dialect_registry)
            pipeline = self._pipeline_constructor(
                *args,
                **kwargs,
            )
            dag = pipeline.build_dag(engine.jinja)

            ctx.obj = GroupContext(engine, dag)

        self.__build_cmd(cli, "run", TaskSequence.run)
        self.__build_cmd(cli, "delete", TaskSequence.delete, ask=True)
        self.__build_cmd(cli, "redo", TaskSequence.redo, ask=True)

        @click.argument("task_name", metavar="TASK", type=TYPE_TREEPATH)
        @cli.command("describe")
        @click.pass_context
        def describe_cmd(click_ctx: click.Context, task_name: TreePath):
            group = click_ctx.find_object(GroupContext)
            assert group, "Group context hasn't been set"

            print_task_info(group.dag.tasks[task_name])

        @click.argument("filename", type=Path, default="graph.dot")
        @cli.command("graph")
        @click.pass_context
        def graph_cmd(click_ctx: click.Context, filename: str):
            group = click_ctx.find_object(GroupContext)
            assert group, "Group context hasn't been set"

            rendered = group.dag.graphviz().render(filename, format="png")
            open_in_default_app(rendered)

        return cli

    def __build_cmd(
        self,
        group: click.Group,
        name: str,
        action: Callable[[TaskSequence, SqlConnection], None],
        ask: bool = False,
    ):
        @click.option(
            "--from",
            "start_from",
            help="run this task and its dependencies",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @click.option(
            "--one",
            "single",
            help="run only this task",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @group.command(name)
        @click.pass_context
        def cmd(
            click_ctx: click.Context,
            start_from: Optional[list[TreePath]],
            single: list[TreePath] = [],
        ):
            group = click_ctx.find_object(GroupContext)
            assert group, "Group context hasn't been set"

            if single:
                sequence = (
                    group.dag.topological_sort_filtered(start_from)
                    if start_from
                    else TaskSequence(
                        [NamedTask(path, group.dag.tasks[path]) for path in single]
                    )
                )
            elif start_from:
                sequence = group.dag.topological_sort_filtered(start_from)
            else:
                sequence = group.dag.topological_sort()

            if ask:
                for task in sequence.steps:
                    console.print(task.name)

            if not ask or Prompt.ask("(y/n)?", console=console) == "y":
                with group.engine.connect() as conn:
                    action(sequence, conn)

    def __call__(self, *args, **kwargs):
        try:
            self.build_cli()(*args, **kwargs)
        except Exception as err:
            traceback_console.print_exception(show_locals=True)
            if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
                traceback_console.print("Row context:", row_context)
            sys.exit(1)


__all__ = ["Ralsei"]
