from typing import Any, MutableMapping, Optional, Protocol
from jinja2.nodes import Template as TemplateNode
from sqlalchemy.sql.elements import TextClause

from ralsei.dialect import DialectInfo

from .environment import SqlTemplate, SqlEnvironment
from .adapter import SqlAdapter


class ISqlEnvironment(Protocol):
    @property
    def adapter(self) -> SqlAdapter: ...

    @property
    def dialect_info(self) -> DialectInfo: ...

    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: None = None,
    ) -> SqlTemplate: ...

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str: ...

    def render_sql(self, source: str, /, *args: Any, **kwargs: Any) -> TextClause: ...

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]: ...

    def render_sql_split(
        self, source: str, /, *args: Any, **kwargs: Any
    ) -> list[TextClause]: ...

    @property
    def base(self) -> SqlEnvironment: ...


__all__ = ["ISqlEnvironment"]
