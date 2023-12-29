from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Type,
    TypeVar,
    overload,
)
import textwrap
import jinja2
from jinja2 import StrictUndefined
from jinja2.environment import TemplateModule
from jinja2.nodes import Template as TemplateNode
import itertools
from contextlib import contextmanager

from .types import Sql, Column, Identifier
from .adapter import create_adapter_for_env
from .sqlalchemy import SqlalchemyEnvironment
from .extensions import SplitTag, SplitMarker
from .compiler import SqlCodeGenerator
from .dialect import DialectInfo

if TYPE_CHECKING:
    from ..pipeline import DependencyResolver, OutputOf


def _render_split(chunks: Iterable[str]) -> list[str]:
    return [
        "".join(group)
        for is_marker, group in itertools.groupby(
            chunks, lambda x: type(x) is SplitMarker
        )
        if not is_marker
    ]


class SqlTemplateModule(TemplateModule):
    def render(self) -> str:
        return str(self)

    def render_split(self) -> list[str]:
        return _render_split(self._body_stream)


class SqlTemplate(jinja2.Template):
    def render_split(self, *args: Any, **kwargs: Any) -> list[str]:
        ctx = self.new_context(dict(*args, **kwargs))

        try:
            return _render_split(self.root_render_func(ctx))
        except Exception:
            self.environment.handle_exception()

    def make_module(
        self,
        vars: Optional[dict[str, Any]] = None,
        shared: bool = False,
        locals: Optional[Mapping[str, Any]] = None,
    ) -> SqlTemplateModule:
        ctx = self.new_context(vars, shared, locals)
        return SqlTemplateModule(self, ctx)


T = TypeVar("T")
TEMPLATE = TypeVar("TEMPLATE", bound=jinja2.Template)


class SqlEnvironment(jinja2.Environment):
    def __init__(self, dialect_info: "DialectInfo"):
        super().__init__(undefined=StrictUndefined)

        self._dependency_resolver: Optional["DependencyResolver"] = None

        self._adapter = create_adapter_for_env(self)
        self._dialect = dialect_info
        self._sqlalchemy = SqlalchemyEnvironment(self)

        def finalize(value: Any) -> str:
            return self.adapter.to_sql(self.resolve(value))

        def joiner(sep: str = ", ") -> Callable[[], Sql]:
            inner = jinja2.utils.Joiner(sep)
            return lambda: Sql(inner())

        def join(
            values: Iterable[Any],
            delimiter: str,
            attribute: Optional[str] = None,
        ) -> Sql:
            return Sql(
                delimiter.join(
                    map(
                        lambda value: self.adapter.to_sql(
                            getattr(value, attribute) if attribute else value
                        ),
                        values,
                    )
                )
            )

        self.finalize = finalize
        self.template_class = SqlTemplate
        self.code_generator_class = SqlCodeGenerator

        self.filters = {
            "sql": Sql,
            "join": join,
            "identifier": Identifier,
        }
        self.globals = {
            "range": range,
            "dict": dict,
            "joiner": joiner,
            "Column": Column,
            "dialect": dialect_info,
        }

        self.add_extension(SplitTag)

    def __repr__(self) -> str:
        return super().__repr__()

    @property
    def adapter(self):
        return self._adapter

    @property
    def dialect(self) -> "DialectInfo":
        return self._dialect

    @property
    def sqlalchemy(self) -> SqlalchemyEnvironment:
        return self._sqlalchemy

    @overload
    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: None = None,
    ) -> SqlTemplate:
        ...

    @overload
    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: Optional[Type[TEMPLATE]] = None,
    ) -> TEMPLATE:
        ...

    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: Optional[Type[jinja2.Template]] = None,
    ) -> jinja2.Template:
        return super().from_string(
            textwrap.dedent(source).strip() if isinstance(source, str) else source,
            globals,
            template_class,
        )

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str:
        return self.from_string(source).render(*args, **kwargs)

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]:
        return self.from_string(source).render_split(*args, **kwargs)

    @overload
    def resolve(self, value: T | "OutputOf") -> T:
        ...

    @overload
    def resolve(self, value: Any) -> Any:
        ...

    def resolve(self, value: Any) -> Any:
        from ..pipeline import OutputOf

        if not isinstance(value, OutputOf):
            return value
        elif self._dependency_resolver:
            return self._dependency_resolver.resolve(self.sqlalchemy, value)
        else:
            raise RuntimeError(
                "attempted to resolve dependency outside of dependency resolution context"
            )

    def getattr(self, obj: Any, attribute: str) -> Any:
        return super().getattr(self.resolve(obj), attribute)

    @contextmanager
    def with_resolver(self, resolver: "DependencyResolver"):
        old_resolver = self._dependency_resolver
        self._dependency_resolver = resolver

        try:
            yield
        finally:
            self._dependency_resolver = old_resolver
