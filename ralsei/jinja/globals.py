from typing import Any, Callable, Iterable, Optional
from typing_extensions import TYPE_CHECKING
import jinja2

from ralsei.types import Sql

if TYPE_CHECKING:
    from .environment import SqlEnvironment


def joiner(sep: str = ", ") -> Callable[[], Sql]:
    inner = jinja2.utils.Joiner(sep)
    return lambda: Sql(inner())


def create_join(env: "SqlEnvironment"):
    def join(
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

    return join
