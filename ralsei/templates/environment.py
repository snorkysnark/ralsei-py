from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Type,
    cast,
)
import textwrap
import jinja2
from jinja2 import Undefined, StrictUndefined
from jinja2.environment import TemplateModule
import itertools
import sqlalchemy

from .adapter import SqlAdapter
from .extensions import append_filter, SplitTag, SplitMarker
from .compiler import SqlCodeGenerator


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


class DialectInfo:
    def __init__(self, sqlalchemy_dialect: sqlalchemy.Dialect) -> None:
        from .types import Sql

        self.sqlalchemy = sqlalchemy_dialect
        self.serial_primary_key = Sql(
            "INTEGER PRIMARY KEY AUTOINCREMENT"
            if sqlalchemy_dialect.name == "sqlite"
            else "SERIAL PRIMARY KEY"
        )

    @property
    def name(self):
        return self.sqlalchemy.name


class SqlEnvironment(jinja2.Environment):
    def __init__(
        self,
        sqlalchemy_dialect: sqlalchemy.Dialect,
        adapter: Optional[SqlAdapter] = None,
    ):
        super().__init__(undefined=StrictUndefined)

        if not adapter:
            adapter = SqlAdapter()
        self._adapter = adapter
        self._dialect = DialectInfo(sqlalchemy_dialect)

        from .types import Sql, Column, Identifier

        def sqltyped(value: Any) -> str | Undefined:
            if isinstance(value, Undefined):
                return value

            return adapter.to_sql(value)

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
                        lambda value: adapter.to_sql(
                            getattr(value, attribute) if attribute else value
                        ),
                        values,
                    )
                )
            )

        self.add_extension(append_filter("sqltyped"))
        self.add_extension(SplitTag)

        self.template_class = SqlTemplate
        self.code_generator_class = SqlCodeGenerator

        self.filters = {
            "sqltyped": sqltyped,
            "sql": Sql,
            "join": join,
            "identifier": Identifier,
        }
        self.globals = {"joiner": joiner, "Column": Column, "dialect": self._dialect}

    @property
    def adapter(self) -> SqlAdapter:
        return self._adapter

    @property
    def dialect(self) -> DialectInfo:
        return self._dialect

    def from_string(
        self,
        source: str,
        globals: Optional[MutableMapping[str, Any]] = None,
        template_class: Optional[Type[SqlTemplate]] = None,
    ) -> SqlTemplate:
        return cast(
            SqlTemplate,
            super().from_string(
                textwrap.dedent(source).strip(), globals, template_class
            ),
        )

    def render(self, source: str, /, *args: Any, **kwargs: Any) -> str:
        return self.from_string(source).render(*args, **kwargs)

    def render_split(self, source: str, /, *args: Any, **kwargs: Any) -> list[str]:
        return self.from_string(source).render_split(*args, **kwargs)
