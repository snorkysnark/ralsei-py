from typing import Any, Callable, Iterable, Optional
from typing_extensions import TYPE_CHECKING
import jinja2

from ralsei.types import Sql, Table, Identifier

if TYPE_CHECKING:
    from .environment import SqlEnvironment


def joiner(sep: str = ", ") -> Callable[[], Sql]:
    inner = jinja2.utils.Joiner(sep)
    return lambda: Sql(inner())


def join(
    env: "SqlEnvironment",
    values: Iterable[Any],
    delimiter: str,
    attribute: Optional[str] = None,
) -> Sql:
    return Sql(
        delimiter.join(
            map(
                lambda value: env.adapter.to_sql(
                    env, getattr(value, attribute) if attribute else value
                ),
                values,
            )
        )
    )


def _render_create_index(
    env: "SqlEnvironment",
    table: Table,
    *column_names: str,
    if_not_exists: bool = False,
    unique: bool = False,
):
    index_name = Table(f"{table.name}_{'_'.join(column_names)}_index", table.schema)
    return index_name, env.render(
        "CREATE INDEX{%if unique%} UNIQUE{%endif%}{%if if_not_exists%} IF NOT EXISTS{%endif%} {{index_name}} ON {{table}}({{columns | join(', ')}});",
        index_name=index_name,
        table=table,
        if_not_exists=if_not_exists,
        unique=unique,
        columns=map(Identifier, column_names),
    )


def create_index(
    env: "SqlEnvironment",
    table: Table,
    *column_names: str,
    if_not_exists: bool = False,
    unique: bool = False,
):
    index_name, statement = _render_create_index(
        env, table, *column_names, if_not_exists=if_not_exists, unique=unique
    )
    return index_name, Sql(statement)


def autoincrement_primary_key(env: "SqlEnvironment", postfix: str = "pkey"):
    if env.dialect.name == "sqlite":
        return Sql("INTEGER PRIMARY KEY AUTOINCREMENT")
    else:
        return Sql("SERIAL PRIMARY KEY")
