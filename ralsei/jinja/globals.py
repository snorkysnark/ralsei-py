from typing import Any, Callable, Iterable, Optional
from typing_extensions import TYPE_CHECKING
import jinja2

from ralsei.types import ToSql, Sql, Table, Identifier
from ralsei.graph import Resolves, DynamicDependency

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


def create_index(env: "SqlEnvironment", table: Table, *column_names: str):
    index_name = Table(f"{table.name}_{'_'.join(column_names)}_index", table.schema)
    return Sql(
        env.render(
            "CREATE INDEX {{index_name}} ON {{table}}({{columns | join(', ')}});",
            index_name=index_name,
            table=table,
            columns=map(Identifier, column_names),
        )
    )


def autoincrement_primary_key(
    env: "SqlEnvironment", table: Optional[Table] = None, suffix: str = "pkey"
) -> Resolves[ToSql]:
    if env.dialect.name == "sqlite":
        return Sql("INTEGER PRIMARY KEY AUTOINCREMENT")
    elif env.dialect.name == "duckdb":
        from ralsei.task import CreateSequence

        if not table:
            raise RuntimeError("Must pass 'table' argument if using duckdb")

        sequence = Table(f"{table.name}_{suffix}", table.schema)
        return DynamicDependency(suffix, CreateSequence(sequence))
    else:
        return Sql("SERIAL PRIMARY KEY")
