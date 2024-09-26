from __future__ import annotations
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Type,
    overload,
)
import textwrap
import jinja2
from jinja2 import StrictUndefined
from jinja2.environment import TemplateModule
from jinja2.nodes import Template as TemplateNode
import itertools
from sqlalchemy import TextClause

from ralsei.dialect import DialectInfo, BaseDialectInfo
from ralsei.types import ToSql, Sql, Column, Identifier
from ralsei.graph import resolve

from .adapter import SqlAdapter
from ._extensions import SplitTag, SplitMarker
from ._compiler import SqlCodeGenerator


def _render_split(chunks: Iterable[str]) -> list[str]:
    return [
        "".join(group)
        for is_marker, group in itertools.groupby(
            chunks, lambda x: type(x) is SplitMarker
        )
        if not is_marker
    ]


class SqlTemplateModule(TemplateModule):
    """Represents an imported template (see :py:attr:`jinja2.Template.module`)

    All the exported names of the template are available as attributes on this object.
    """

    def render(self) -> str:
        """Render as string"""

        return str(self)

    def render_sql(self) -> TextClause:
        """Render and wrap with :py:func:`sqlalchemy.sql.expression.text`"""

        return TextClause(str(self))

    def render_split(self) -> list[str]:
        """Render as multiple statements, splitting on ``{%split%}`` tag"""

        return _render_split(self._body_stream)

    def render_sql_split(self) -> list[TextClause]:
        """Render as multiple statements, splitting on ``{%split%}`` tag, wrap with :py:func:`sqlalchemy.sql.expression.text`"""

        return list(map(TextClause, self.render_split()))


class SqlTemplate(jinja2.Template):
    """Compiled jinja template that can be rendered"""

    @classmethod
    def _from_namespace(
        cls,
        environment: jinja2.Environment,
        namespace: MutableMapping[str, Any],
        globals: MutableMapping[str, Any],
    ) -> jinja2.Template:
        return super(SqlTemplate, cls)._from_namespace(environment, namespace, globals)

    def render_sql(self, *args: Any, **kwargs: Any) -> TextClause:
        """Render and wrap in :py:func:`sqlalchemy.sql.expression.text`"""

        return TextClause(self.render(*args, **kwargs))

    def render_split(self, *args: Any, **kwargs: Any) -> list[str]:
        """Render as multiple statements, splitting on ``{%split%}`` tag"""

        ctx = self.new_context(dict(*args, **kwargs))

        try:
            return _render_split(self.root_render_func(ctx))
        except Exception:
            self.environment.handle_exception()

    def render_sql_split(self, *args: Any, **kwargs: Any) -> list[TextClause]:
        """Render as multiple statements, splitting on ``{%split%}`` tag, wrap in :py:func:`sqlalchemy.sql.expression.text`"""

        return list(map(TextClause, self.render_split(*args, **kwargs)))

    def make_module(
        self,
        vars: Optional[dict[str, Any]] = None,
        shared: bool = False,
        locals: Optional[Mapping[str, Any]] = None,
    ) -> SqlTemplateModule:
        """The template as a module, use this to access exported template variables"""

        ctx = self.new_context(vars, shared, locals)
        return SqlTemplateModule(self, ctx)


def create_adapter(env: SqlEnvironment):
    adapter = SqlAdapter()
    adapter.register_type(str, lambda value: "'{}'".format(value.replace("'", "''")))
    adapter.register_type(int, str)
    adapter.register_type(float, str)
    adapter.register_type(type(None), lambda value: "NULL")
    adapter.register_type(ToSql, lambda value: value.to_sql(env))

    return adapter


class SqlEnvironment(jinja2.Environment):
    """Type-aware jinja environment for rendering SQL

    Args:
        dialect_info: dialect-specific settings
    """

    def __init__(self, dialect_info: DialectInfo = BaseDialectInfo):
        super().__init__(undefined=StrictUndefined)

        self._adapter = create_adapter(self)
        self._dialect_info = dialect_info

        def finalize(value: Any) -> str | jinja2.Undefined:
            if isinstance(value, jinja2.Undefined):
                return value
            else:
                return self.adapter.to_sql(resolve(self, value))

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

    @property
    def adapter(self) -> SqlAdapter:
        """Type adapter that turns values in braces (like ``{{value}}``) into SQL strings"""

        return self._adapter

    @property
    def dialect_info(self) -> DialectInfo:
        """Dialect-specific settings"""

        return self._dialect_info

    @overload
    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: None = None,
    ) -> SqlTemplate: ...

    @overload
    def from_string[
        TEMPLATE: jinja2.Template
    ](
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: Optional[type[TEMPLATE]] = None,
    ) -> TEMPLATE: ...

    def from_string(
        self,
        source: str | TemplateNode,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: Optional[Type[jinja2.Template]] = None,
    ) -> jinja2.Template:
        """See :py:meth:`jinja2.Environment.from_string`

        By default, the template class will be :py:class:`SqlTemplate`
        """

        return super().from_string(
            textwrap.dedent(source).strip() if isinstance(source, str) else source,
            globals,
            template_class,
        )

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str:
        """Render template once, shorthand for ``self.from_string().render()``"""

        return self.from_string(source).render(*args, **kwargs)

    def render_sql(self, source: str, /, *args: Any, **kwargs: Any) -> TextClause:
        """Render and wrap with :py:func:`sqlalchemy.sql.expression.text`"""

        return self.from_string(source).render_sql(*args, **kwargs)

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]:
        """Render as multiple statements, splitting on ``{%split%}`` tag"""

        return self.from_string(source).render_split(*args, **kwargs)

    def render_sql_split(
        self, source: str, /, *args: Any, **kwargs: Any
    ) -> list[TextClause]:
        """Render as multiple statements, splitting on ``{%split%}`` tag, wrap with :py:func:`sqlalchemy.sql.expression.text`"""

        return self.from_string(source).render_sql_split(*args, **kwargs)

    def getattr(self, obj: Any, attribute: str) -> Any:
        return super().getattr(resolve(self, obj), attribute)

    @property
    def base(self) -> SqlEnvironment:
        return self


__all__ = ["SqlTemplateModule", "SqlTemplate", "SqlEnvironment"]
