from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    Optional,
    TypeVar,
    overload,
)
from sqlalchemy import TextClause, text

if TYPE_CHECKING:
    from ralsei.sql_adapter import SqlAdapter
    from ralsei.dialect import DialectInfo
    from ralsei.graph import DependencyResolver, OutputOf

if TYPE_CHECKING:
    from .environment import (
        SqlEnvironment,
        SqlTemplate,
        SqlTemplateModule,
    )


class SqlalchemyTemplateModule:
    def __init__(self, inner: "SqlTemplateModule") -> None:
        self._inner = inner

    def render(self) -> TextClause:
        return text(self._inner.render())

    def render_split(self) -> list[TextClause]:
        return list(map(text, self._inner.render_split()))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class SqlalchemyTemplate:
    def __init__(self, inner: "SqlTemplate") -> None:
        self._inner = inner

    def make_module(
        self,
        vars: Optional[dict[str, Any]] = None,
        shared: bool = False,
        locals: Optional[Mapping[str, Any]] = None,
    ) -> SqlalchemyTemplateModule:
        return SqlalchemyTemplateModule(self._inner.make_module(vars, shared, locals))

    def render(self, *args: Any, **kwargs: Any) -> TextClause:
        return text(self._inner.render(*args, **kwargs))

    def render_split(self, *args: Any, **kwargs: Any) -> list[TextClause]:
        return list(map(text, self._inner.render_split(*args, **kwargs)))


T = TypeVar("T")


class SqlalchemyEnvironment:
    def __init__(self, inner: "SqlEnvironment") -> None:
        self._inner = inner

    @property
    def text(self) -> "SqlEnvironment":
        return self._inner

    def from_string(
        self,
        source: str,
        globals: Optional[MutableMapping[str, Any]] = None,
    ) -> SqlalchemyTemplate:
        return SqlalchemyTemplate(self._inner.from_string(source, globals))

    @property
    def adapter(self) -> "SqlAdapter":
        return self._inner.adapter

    @property
    def dialect(self) -> DialectInfo:
        return self._inner.dialect

    def render(self, source: str, /, *args, **kwargs) -> TextClause:
        return text(self._inner.render(source, *args, **kwargs))

    def render_split(self, source: str, /, *args, **kwargs) -> list[TextClause]:
        return list(map(text, self._inner.render_split(source, *args, **kwargs)))

    @overload
    def resolve(self, value: T | OutputOf) -> T:
        ...

    @overload
    def resolve(self, value: Any) -> Any:
        ...

    def resolve(self, value: Any) -> Any:
        return self._inner.resolve(value)

    def with_resolver(self, resolver: "DependencyResolver"):
        return self._inner.with_resolver(resolver)


__all__ = ["SqlalchemyTemplateModule", "SqlalchemyTemplate", "SqlalchemyEnvironment"]
