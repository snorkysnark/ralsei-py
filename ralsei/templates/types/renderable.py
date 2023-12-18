from typing import Protocol, TypeVar, Any, Self

from ..environment import SqlEnvironment


T = TypeVar("T", covariant=True)


class Renderable(Protocol[T]):
    def render(self, env: SqlEnvironment, /, **params: Any) -> T:
        ...


class RendersToSelf:
    def render(self, env: SqlEnvironment, /, **params: Any) -> Self:
        return self
