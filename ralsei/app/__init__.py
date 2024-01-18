from dataclasses import dataclass
import click
from rich import traceback
from typing import Callable, Optional, Sequence, overload
from returns.maybe import Maybe
import inspect

from ralsei.dialect import DialectRegistry, Dialect
from ralsei.graph import Pipeline, TreePath, TaskSequence, DAG
from ralsei.context import ConnectionContext, EngineContext
from ._parsers import TYPE_TREEPATH
from ._utils import extend_params


@dataclass
class GroupContext:
    engine: EngineContext
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
            self._engine_context_arg = None
        else:
            self._pipeline_constructor = pipeline_source
            self._custom_cli_options = [
                *getattr(pipeline_source, "__click_params__", []),
                *custom_cli_options,
            ]

            for name, param in inspect.signature(pipeline_source).parameters.items():
                if param.annotation is EngineContext:
                    self._engine_context_arg = name
                    break

        self._dialect_registry = DialectRegistry.create_default()

    def register_dialect(
        self,
        dialect_name: str,
        dialect_class: type[Dialect],
        driver: Optional[str] = None,
    ):
        self._dialect_registry.register_dialect(
            dialect_name, dialect_class, driver=driver
        )

    def build_cli(self) -> click.Group:
        @extend_params(self._custom_cli_options)
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        @click.pass_context
        def cli(ctx: click.Context, db: str, *args, **kwargs):
            engine = EngineContext.create(db, dialect=self._dialect_registry)
            pipeline = self._pipeline_constructor(
                *args,
                **kwargs,
                **Maybe.from_optional(self._engine_context_arg)
                .map(lambda arg: {arg: engine})
                .value_or({}),
            )
            dag = pipeline.build_dag(engine.jinja)

            ctx.obj = GroupContext(engine, dag)

        for name, action in [
            ("run", TaskSequence.run),
            ("delete", TaskSequence.delete),
            ("redo", TaskSequence.redo),
        ]:
            self.__build_cmd(cli, name, action)

        return cli

    def __build_cmd(
        self,
        group: click.Group,
        name: str,
        action: Callable[[TaskSequence, ConnectionContext], None],
    ):
        @click.option(
            "--from",
            "start_from",
            help="run this task and its dependencies",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @group.command(name)
        @click.pass_context
        def cmd(click_ctx: click.Context, start_from: Optional[list[TreePath]]):
            group = click_ctx.find_object(GroupContext)
            assert group, "Group context hasn't been set"

            sequence = group.dag.topological_sort(start_from=start_from)
            with group.engine.connect() as ctx:
                action(sequence, ctx)

    def __call__(self, *args, **kwargs):
        traceback.install(show_locals=True)
        self.build_cli()(*args, **kwargs)


__all__ = ["Ralsei"]
