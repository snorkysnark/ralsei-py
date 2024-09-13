from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ralsei.jinja import ISqlEnvironment


class Renderable[T](Protocol):
    def render(self, env: "ISqlEnvironment", /, **params: Any) -> T: ...
