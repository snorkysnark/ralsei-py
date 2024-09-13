from dataclasses import dataclass
from pathlib import Path
import sys
import click
from rich.console import Console
from rich.prompt import Prompt
from typing import Callable, Iterable, Sequence, overload
import sqlalchemy

from ralsei.graph import Pipeline, TreePath, TaskSequence, DAG
from ralsei.connection import create_engine, ConnectionEnvironment
from ralsei.console import console
from ralsei.task import ROW_CONTEXT_ATRRIBUTE
from ralsei.jinja import SqlEnvironment
from ralsei.dialect import get_dialect

from ._parsers import TYPE_TREEPATH
from ._decorators import extend_params
from ._rich import print_task_info
from ._opener import open_in_default_app

traceback_console = Console(stderr=True)


@dataclass
class GroupContext:
    engine: sqlalchemy.Engine
    dag: DAG


class Ralsei:
    @overload
    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline],
        custom_cli_options: Sequence[click.Option] = [],
    ) -> None: ...

    @overload
    def __init__(self, pipeline_source: Pipeline) -> None: ...

    def __init__(
        self,
        pipeline_source: Callable[..., Pipeline] | Pipeline,
        custom_cli_options: Sequence[click.Option] = [],
    ) -> None:
        if isinstance(pipeline_source, Pipeline):
            self._pipeline_factory = lambda: pipeline_source
            self._custom_cli_options = []
        else:
            self._pipeline_factory = pipeline_source
            self._custom_cli_options = [
                *getattr(pipeline_source, "__click_params__", []),
                *custom_cli_options,
            ]

    def build_cli(self) -> click.Group:
        @extend_params(self._custom_cli_options)
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        @click.pass_context
        def cli(ctx: click.Context, db: str, *args, **kwargs):
            engine = create_engine(db)
            pipeline = self._pipeline_factory(
                *args,
                **kwargs,
            )
            dag = pipeline.build_dag(SqlEnvironment(get_dialect(engine.dialect.name)))

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
        action: Callable[[TaskSequence, ConnectionEnvironment], None],
        ask: bool = False,
    ):
        @click.option(
            "--from",
            "from_filters",
            help="run this task and its dependencies",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @click.option(
            "--one",
            "single_filters",
            help="run only this task",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @group.command(name)
        @click.pass_context
        def cmd(
            click_ctx: click.Context,
            from_filters: Iterable[TreePath],
            single_filters: Iterable[TreePath],
        ):
            group = click_ctx.find_object(GroupContext)
            assert group, "Group context hasn't been set"

            sequence = group.dag.topological_sort()

            if from_filters or single_filters:
                mask: set[TreePath] = set()

                for task in group.dag.topological_sort(
                    constrain_starting_nodes=from_filters
                ).steps:
                    mask.add(task.path)
                for single_path in single_filters:
                    mask.add(single_path)

                sequence = TaskSequence(
                    [task for task in sequence.steps if task.path in mask]
                )

            if (
                not ask
                or Prompt.ask(
                    "\n".join([*(task.name for task in sequence.steps), "(y/n)?"]),
                    console=console,
                )
                == "y"
            ):
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
