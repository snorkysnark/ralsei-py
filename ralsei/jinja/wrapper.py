from typing import TYPE_CHECKING, Any, MutableMapping, Optional
from jinja2.nodes import Template as TemplateNode
from sqlalchemy.sql.elements import TextClause

from ralsei.dialect import DialectInfo

if TYPE_CHECKING:
    from .environment import SqlEnvironment, SqlTemplate
    from .adapter import SqlAdapter


class SqlEnvironmentWrapper:
    def __init__(self, env: "SqlEnvironment", locals: dict[str, Any]) -> None:
        self.__inner = env
        self.__locals = locals

    @property
    def adapter(self) -> "SqlAdapter":
        return self.__inner.adapter

    @property
    def dialect_info(self) -> DialectInfo:
        return self.__inner.dialect_info

    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: None = None,
    ) -> "SqlTemplate":
        return self.__inner.from_string(
            source, {**self.__locals, **(globals or {})}, template_class
        )

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str:
        return self.__inner.render(source, *args, **self.__locals, **kwargs)

    def render_sql(self, source: str, /, *args: Any, **kwargs: Any) -> TextClause:
        return self.__inner.render_sql(source, *args, **self.__locals, **kwargs)

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]:
        return self.__inner.render_split(source, *args, **self.__locals, **kwargs)

    def render_sql_split(
        self, source: str, /, *args: Any, **kwargs: Any
    ) -> list[TextClause]:
        return self.__inner.render_sql_split(source, *args, **self.__locals, **kwargs)

    @property
    def base(self) -> "SqlEnvironment":
        return self.__inner


__all__ = ["SqlEnvironmentWrapper"]
