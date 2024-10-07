from typing import Any, MutableMapping, Optional, Protocol
from jinja2.nodes import Template as TemplateNode
from sqlalchemy.sql.elements import TextClause

from ralsei.dialect import DialectInfo

from .environment import SqlTemplate, SqlEnvironment
from .adapter import SqlAdapter


class ISqlEnvironment(Protocol):
    """Interface describing a :py:class:`ralsei.jinja.SqlEnvironment` like object

    (including :py:class:`ralsei.jinja.SqlEnvironmentWrapper`)"""

    @property
    def adapter(self) -> SqlAdapter:
        """Type adapter that turns values in braces (like ``{{value}}``) into SQL strings"""
        ...

    @property
    def dialect_info(self) -> DialectInfo:
        """Dialect-specific settings"""
        ...

    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: None = None,
    ) -> SqlTemplate:
        """See :py:meth:`jinja2.Environment.from_string`

        By default, the template class will be :py:class:`SqlTemplate`
        """
        ...

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str:
        """Render template once, shorthand for ``self.from_string().render()``"""
        ...

    def render_sql(self, source: str, /, *args: Any, **kwargs: Any) -> TextClause:
        """Render and wrap with :py:func:`sqlalchemy.sql.expression.text`"""
        ...

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]:
        """Render as multiple statements, splitting on ``{%split%}`` tag"""
        ...

    def render_sql_split(
        self, source: str, /, *args: Any, **kwargs: Any
    ) -> list[TextClause]:
        """Render as multiple statements, splitting on ``{%split%}`` tag, wrap with :py:func:`sqlalchemy.sql.expression.text`"""
        ...

    @property
    def base(self) -> SqlEnvironment:
        """The base environment"""
        ...


__all__ = ["ISqlEnvironment"]
