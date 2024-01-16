import click
from rich import traceback
from typing import Any, Callable, Optional, Sequence, overload
import itertools
from ralsei.dialect import DialectRegistry, Dialect

from ralsei.graph import Pipeline, TreePath
from ralsei.context import EngineContext


def get_existing_names(params: Sequence[click.Parameter]) -> tuple[set[str], set[str]]:
    existing_names = set()
    existing_opts = set()
    for param in params:
        if param.name:
            existing_names.add(param.name)
        for opt in param.opts:
            existing_opts.add(opt)
        for opt in param.secondary_opts:
            existing_opts.add(opt)

    return existing_names, existing_opts


def extend_params(
    extra_params: Sequence[click.Parameter],
) -> Callable[[click.Command], click.Command]:
    def decorator(cmd: click.Command) -> click.Command:
        existing_names, existing_opts = get_existing_names(cmd.params)
        for param in extra_params:
            if param.name and param.name in existing_names:
                raise ValueError(f"parameter name {param.name} already occupied")
            for opt in itertools.chain(param.opts, param.secondary_opts):
                if opt in existing_opts:
                    raise ValueError(f"option name {opt} already occupied")

            cmd.params.append(param)

        return cmd

    return decorator


class TreePathType(click.ParamType):
    name = "treepath"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, TreePath):
            return value
        elif isinstance(value, str):
            return TreePath.parse(value)
        else:
            self.fail("Must be of type str or TreePath")


TYPE_TREEPATH = TreePathType()


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
        @click.group(context_settings=dict(help_option_names=["-h", "--help"]))
        def cli():
            pass

        @extend_params(self._custom_cli_options)
        @click.option("--db", "-d", help="sqlalchemy database url", required=True)
        @click.option(
            "--from",
            "start_from",
            help="run this task and its dependencies",
            type=TYPE_TREEPATH,
            multiple=True,
        )
        @cli.command("run")
        def run_cmd(db: str, start_from: Optional[list[TreePath]], *args, **kwargs):
            pipeline = self._pipeline_constructor(*args, **kwargs)
            engine = EngineContext.create(db, dialect=self._dialect_registry)
            dag = pipeline.build_dag(engine.jinja)
            sequence = dag.topological_sort(start_from=start_from)

            with engine.connect() as ctx:
                sequence.run(ctx)

        return cli

    def __call__(self, *args, **kwargs):
        traceback.install(show_locals=True)
        self.build_cli()(*args, **kwargs)


__all__ = ["Ralsei"]
