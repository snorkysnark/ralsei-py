from typing import Any, Callable
import itertools
import inspect


class DIContainer:
    def __init__(
        self, factories: dict[type, Callable[["DIContainer"], Any]] | None = None
    ) -> None:
        self._factories = factories or {}

    def get[T](self, type_: type[T]) -> T:
        if factory := self._factories.get(type_, None):
            return factory(self)

        raise RuntimeError(f"Service of type {type_} not found")

    def bind_factory[T](self, type_: type[T], func: Callable[..., T]):
        signature = inspect.signature(func, eval_str=True)

        self._factories[type_] = lambda di: func(
            **{
                key: di.get(param.annotation)
                for key, param in signature.parameters.items()
            }
        )

    def bind_value[T](self, type_: type[T], value: T):
        self._factories[type_] = lambda di: value

    def execute[T](self, func: Callable[..., T], *args) -> T:
        signature = inspect.signature(func, eval_str=True)
        # Annotations for remaining parameters (that aren't provided by args)
        parameters_rest = itertools.islice(
            signature.parameters.items(), len(args), None
        )

        return func(
            *args, **{key: self.get(param.annotation) for key, param in parameters_rest}
        )

    def clone(self) -> "DIContainer":
        return DIContainer({**self._factories})
