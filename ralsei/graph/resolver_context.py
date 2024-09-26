from typing import TYPE_CHECKING, Any, overload

from .outputof import OutputOf
from .error import ResolverContextError
from ._resolver import RESOLVER_CONTEXT

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


@overload
def resolve[T](env: "ISqlEnvironment", value: T | OutputOf) -> T: ...


@overload
def resolve(env: "ISqlEnvironment", value: Any) -> Any: ...


def resolve(env: "ISqlEnvironment", value: Any) -> Any:
    """If ``value`` is :py:class:`ralsei.graph.OutputOf`, resolve it. Otherwise, returns ``value``

    Can only be called from :py:meth:`ralsei.task.TaskImpl.prepare`"""

    if not isinstance(value, OutputOf):
        return value
    elif resolver := RESOLVER_CONTEXT.get(None):
        return resolver.resolve(env, value)
    else:
        raise ResolverContextError(
            "attempted to resolve dependency outside of dependency resolution context"
        )


__all__ = ["resolve"]
