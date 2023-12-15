from typing import Any, Callable, Iterable, MutableMapping, Optional, Type, cast
import textwrap
import jinja2
from jinja2 import Undefined, StrictUndefined

from .adapter import SqlAdapter
from .extension import append_filter
from .compiler import SqlCodeGenerator

SPLIT_STR = "split_6879f248c0234078baf0eb7c0f2787a1"


class SqlTemplate(jinja2.Template):
    def render_split(self, *args: Any, **kwargs: Any) -> list[str]:
        from .types import Sql

        return super().render(*args, **kwargs, split=Sql(SPLIT_STR)).split(SPLIT_STR)


class SqlEnvironment(jinja2.Environment):
    def __init__(self, adapter: SqlAdapter):
        super().__init__(undefined=StrictUndefined)

        self._adapter = adapter

        from .types import Sql, Column

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
        self.template_class = SqlTemplate
        self.code_generator_class = SqlCodeGenerator

        self.filters = {"sqltyped": sqltyped, "sql": Sql, "join": join}
        self.globals = {"joiner": joiner, "Column": Column}

    @property
    def adapter(self) -> SqlAdapter:
        return self._adapter

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

    def render(self, source: str, /, *args, **kwargs) -> str:
        return self.from_string(source).render(*args, **kwargs)

    def render_split(self, source: str, /, *args, **kwargs) -> list[str]:
        return self.from_string(source).render_split(*args, **kwargs)
