import sys
import click
from pathlib import Path
from typing import Callable, Sequence
from rich.console import Console
from rich.prompt import Prompt
import sqlalchemy

from ralsei.graph import Pipeline, TreePath, TaskSequence, DAG
from ralsei.connection import (
    create_engine as create_engine_default,
    ConnectionEnvironment,
    ConnectionExt,
)
from ralsei.task.rowcontext import ROW_CONTEXT_ATRRIBUTE
from ralsei.jinja import SqlEnvironment
from ralsei.dialect import get_dialect
from ralsei.utils import expect

from ._parsers import type_treepath, type_sqlalchemy_url
from ._decorators import extend_params
from ._rich import print_task_scripts
from ._opener import open_in_default_app

traceback_console = Console(stderr=True)


def confirm_sequence(sequence: TaskSequence):
    return (
        Prompt.ask(
            "\n".join([*(task.name for task in sequence.steps), "(y/n)?"]),
        )
        == "y"
    )


class Ralsei:
    """The pipeline-running CLI application

    Decorate your subclass with :py:func:`click.option` decorator
    to add custom `CLI options <https://click.palletsprojects.com/en/8.1.x/options/>`_.
    Positional arguments are not allowed

    Args:
        url: When the class constructor is called by the CLI,
            the URL is provided as the first argument
        pipeline: The CLI **does not** give you the pipeline,
            you must create one in your subclass and pass it to ``super().__init__()``

    Example:
        .. code-block:: python

            @click.option("-s", "--schema", help="Database schema")
            class App(Ralsei):
                def __init__(
                    url: sqlalchemy.URL, # First argument must always be the url
                    schema: str | None, # Custom argument added with the click decorator
                ):
                    super().__init__(url, MyPipeline(schema))

            if __name__ == "__main__":
                App.run_cli()
    """

    pipeline: Pipeline
    engine: sqlalchemy.Engine
    env: SqlEnvironment
    dag: DAG

    def __init__(self, url: sqlalchemy.URL, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.engine = self._create_engine(url)

        env = SqlEnvironment(get_dialect(self.engine.dialect.name))
        self._prepare_env(env)
        self.env = env

        self.dag = pipeline.build_dag(self.env)

    def _create_engine(self, url: sqlalchemy.URL) -> sqlalchemy.Engine:
        """Override this to customize engine creation"""

        return create_engine_default(url)

    def _prepare_env(self, env: SqlEnvironment):
        """Here you can add your own filters/globals to the jinja environment"""

    def connect(self) -> ConnectionEnvironment:
        """Creates a new connection, returns connection + jinja env"""

        conn = ConnectionEnvironment(self.engine, self.env)
        self._on_connect(conn)
        return conn

    def _on_connect(self, conn: ConnectionEnvironment):
        """Run custom code after database connection"""

    @classmethod
    def build_cli(cls) -> click.Group:
        """Create a click CLI based on this class"""

        custom_args = getattr(cls, "__click_params__", [])
        for arg in custom_args:
            if not isinstance(arg, click.Option):
                raise ValueError("Only Option arguments are permitted")

        @extend_params(getattr(cls, "__click_params__", []))
        @click.option(
            "--db",
            "-d",
            type=type_sqlalchemy_url,
            help="sqlalchemy database url",
            required=True,
        )
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        @click.pass_context
        def cli(ctx: click.Context, db: sqlalchemy.URL, **kwargs):
            ctx.obj = cls(db, **kwargs)

        cls.__build_subcommand(cli, "run", TaskSequence.run)
        cls.__build_subcommand(cli, "delete", TaskSequence.delete, ask=True)
        cls.__build_subcommand(cli, "redo", TaskSequence.redo, ask=True)

        @click.argument("task_name", metavar="TASK", type=type_treepath)
        @cli.command("describe")
        @click.pass_context
        def describe_cmd(ctx: click.Context, task_name: TreePath):
            this = expect(
                ctx.find_object(Ralsei), RuntimeError("click context not set")
            )
            print_task_scripts(this.dag.tasks[task_name])

        @click.argument("filename", type=Path, default="graph.dot")
        @cli.command("graph")
        @click.pass_context
        def graph_cmd(ctx: click.Context, filename: str):
            this = expect(
                ctx.find_object(Ralsei), RuntimeError("click context not set")
            )

            rendered = this.dag.graphviz().render(filename, format="png")
            open_in_default_app(rendered)

        return cli

    @staticmethod
    def __build_subcommand(
        group: click.Group,
        name: str,
        action: Callable[[TaskSequence, ConnectionExt], None],
        ask: bool = False,
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
            this = expect(
                ctx.find_object(Ralsei), RuntimeError("click context not set")
            )

            sequence = this.dag.sort_filtered(from_filters, single_filters)
            if not ask or confirm_sequence(sequence):
                with this.connect() as conn:
                    action(sequence, conn.sqlalchemy)

    @classmethod
    def run_cli(cls, *args, **kwargs):
        """Build and run click CLI, print traceback in case of exception"""

        try:
            cls.build_cli()(*args, **kwargs)
        except Exception as err:
            traceback_console.print_exception(show_locals=True)
            if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
                traceback_console.print("Row context:", row_context)
            sys.exit(1)


__all__ = ["Ralsei"]
