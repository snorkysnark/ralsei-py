from typing import TYPE_CHECKING, Any, TypeVar, overload

from .outputof import OutputOf
from ._resolver import RESOLVER_CONTEXT, CyclicGraphError

if TYPE_CHECKING:
    from ralsei.jinja import SqlEnvironment

T = TypeVar("T")


class ResolverContextError(RuntimeError):
    pass


@overload
def resolve(env: "SqlEnvironment", value: T | OutputOf) -> T:
    ...


@overload
def resolve(env: "SqlEnvironment", value: Any) -> Any:
    ...


def resolve(env: "SqlEnvironment", value: Any) -> Any:
    if not isinstance(value, OutputOf):
        return value
    elif resolver := RESOLVER_CONTEXT.get(None):
        return resolver.resolve(env, value)
    else:
        raise ResolverContextError(
            "attempted to resolve dependency outside of dependency resolution context"
        )


__all__ = ["resolve", "ResolverContextError", "CyclicGraphError"]
