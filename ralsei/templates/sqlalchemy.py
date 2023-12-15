from typing import Any, Mapping, MutableMapping, Optional
from sqlalchemy import TextClause, text

from .environment import SqlEnvironment, SqlTemplate, SqlTemplateModule
from .adapter import SqlAdapter


class SqlalchemyTemplateModule:
    def __init__(self, inner: SqlTemplateModule) -> None:
        self.inner = inner

    def render(self) -> TextClause:
        return text(self.inner.render())

    def render_split(self) -> list[TextClause]:
        return list(map(text, self.inner.render_split()))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class SqlalchemyTemplate:
    def __init__(self, inner: SqlTemplate) -> None:
        self.inner = inner

    def make_module(
        self,
        vars: Optional[dict[str, Any]] = None,
        shared: bool = False,
        locals: Optional[Mapping[str, Any]] = None,
    ) -> SqlalchemyTemplateModule:
        return SqlalchemyTemplateModule(self.inner.make_module(vars, shared, locals))

    def render(self, *args: Any, **kwargs: Any) -> TextClause:
        return text(self.inner.render(*args, **kwargs))

    def render_split(self, *args: Any, **kwargs: Any) -> list[TextClause]:
        return list(map(text, self.inner.render_split(*args, **kwargs)))


class SqlalchemyEnvironment:
    def __init__(self, inner: SqlEnvironment) -> None:
        self.inner = inner

    def from_string(
        self,
        source: str,
        globals: Optional[MutableMapping[str, Any]] = None,
    ) -> SqlalchemyTemplate:
        return SqlalchemyTemplate(self.inner.from_string(source, globals))

    @property
    def adapter(self) -> SqlAdapter:
        return self.inner.adapter

    def render(self, source: str, /, *args, **kwargs) -> TextClause:
        return text(self.inner.render(source, *args, **kwargs))

    def render_split(self, source: str, /, *args, **kwargs) -> list[TextClause]:
        return list(map(text, self.inner.render_split(source, *args, **kwargs)))
