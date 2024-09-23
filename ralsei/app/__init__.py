import sys
import click
from pathlib import Path
from typing import Sequence
from rich.console import Console
from rich.prompt import Prompt

from ralsei.graph import Pipeline, TreePath, TaskSequence
from ralsei.connection import create_engine, ConnectionEnvironment
from ralsei.task import ROW_CONTEXT_ATRRIBUTE
from ralsei.jinja import SqlEnvironment
from ralsei.dialect import get_dialect
from ralsei.utils import expect

from ._parsers import TYPE_TREEPATH
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
    def __init__(self, db: str, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.engine = create_engine(db)

        env = SqlEnvironment(get_dialect(self.engine.dialect.name))
        self._prepare_env(env)
        self.env = env

        self.dag = pipeline.build_dag(self.env)

    def _prepare_env(self, env: SqlEnvironment):
        pass

    def connect(self) -> ConnectionEnvironment:
        conn = ConnectionEnvironment(self.engine, self.env)
        self._on_connect(conn)
        return conn

    def _on_connect(self, conn: ConnectionEnvironment):
        pass

    @classmethod
    def build_cli(cls) -> click.Group:
        @extend_params(getattr(cls, "__click_params__", []))
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        @click.pass_context
        def cli(ctx: click.Context, db: str, *args, **kwargs):
            ctx.obj = cls(db, *args, **kwargs)

        for name, action, ask in [
            ("run", TaskSequence.run, False),
            ("delete", TaskSequence.delete, True),
            ("redo", TaskSequence.redo, True),
        ]:

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
            @cli.command(name)
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

        @click.argument("task_name", metavar="TASK", type=TYPE_TREEPATH)
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

    @classmethod
    def run_cli(cls, *args, **kwargs):
        try:
            cls.build_cli()(*args, **kwargs)
        except Exception as err:
            traceback_console.print_exception(show_locals=True)
            if row_context := getattr(err, ROW_CONTEXT_ATRRIBUTE, None):
                traceback_console.print("Row context:", row_context)
            sys.exit(1)


__all__ = ["Ralsei"]
