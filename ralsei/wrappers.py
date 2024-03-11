from abc import ABC, abstractmethod
import contextlib
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    overload,
)
from functools import wraps

POPPED_FIELDS_ATTR = "__ralsei_popped_fields"

OneToOne = Callable[..., dict[str, Any]]
OneToMany = Callable[..., Iterator[dict[str, Any]]]


def into_many(fn: OneToOne) -> OneToMany:
    @wraps(fn)
    def wrapper(**kwargs: Any):
        yield fn(**kwargs)

    return wrapper


def into_one(fn: OneToMany) -> OneToOne:
    @wraps(fn)
    def wrapper(**kwargs: Any):
        generator = fn(**kwargs)
        first_value = next(generator)

        # If there's more than one value in the generator, throw an error
        try:
            next(generator)
        except StopIteration:
            return first_value

        raise ValueError("Generator returned more than one value")

    return wrapper


def pop_id_fields(*id_fields: str, keep: bool = False):
    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            id_values = {
                name: (kwargs[name] if keep else kwargs.pop(name)) for name in id_fields
            }
            for row in fn(**kwargs):
                yield {**row, **id_values}

        # Save metadata on which fields are considered identifiers (useful for SQL generation)
        metadata = getattr(wrapper, POPPED_FIELDS_ATTR, [])
        metadata.extend(id_fields)
        setattr(wrapper, POPPED_FIELDS_ATTR, metadata)

        return wrapper

    return decorator


def rename_input(mapping: dict[str, str] | Callable[[str], str]):
    remap = (lambda key: mapping[key]) if isinstance(mapping, dict) else mapping

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            yield from fn(**{remap(key): value for key, value in kwargs.items()})

        return wrapper

    return decorator


def rename_output(mapping: dict[str, str] | Callable[[str], str]):
    remap = (lambda key: mapping[key]) if isinstance(mapping, dict) else mapping

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            for row in fn(**kwargs):
                yield {remap(key): value for key, value in row.items()}

        return wrapper

    return decorator


def add_to_input(**add_values):
    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            yield from fn(**kwargs, **add_values)

        return wrapper

    return decorator


def add_to_output(**add_values):
    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            for row in fn(**kwargs):
                yield {**row, **add_values}

        return wrapper

    return decorator


T = TypeVar("T")


class FnContextBase(ABC, Generic[T]):
    def __init__(self, fn: T, **context: Any) -> None:
        self.fn = fn
        self._context_managers = {
            name: obj if hasattr(obj, "__enter__") else contextlib.nullcontext(obj)
            for name, obj in context.items()
        }

    def __enter__(self) -> T:
        context = {
            name: obj.__enter__() for name, obj in self._context_managers.items()
        }
        return self._wrap_fn(context)

    @abstractmethod
    def _wrap_fn(self, context: dict[str, Any]) -> T:
        ...

    def __exit__(self, *excinfo):
        for context_manager in self._context_managers.values():
            context_manager.__exit__(*excinfo)


class FnContext(FnContextBase[OneToMany]):
    def _wrap_fn(self, context: dict[str, Any]) -> OneToMany:
        return add_to_input(**context)(self.fn)


class FnContextOne(FnContextBase[OneToOne]):
    def _wrap_fn(self, context: dict[str, Any]) -> OneToOne:
        @wraps(self.fn)
        def wrapper(**kwargs):
            return self.fn(**kwargs, **context)

        return wrapper


@overload
def compose(
    fn: OneToMany,
    *decorators: Callable[[OneToMany], OneToMany],
    context: None = None,
) -> OneToMany:
    ...


@overload
def compose(
    fn: OneToMany,
    *decorators: Callable[[OneToMany], OneToMany],
    context: dict,
) -> FnContext:
    ...


def compose(
    fn: OneToMany,
    *decorators: Callable[[OneToMany], OneToMany],
    context: Optional[dict] = None,
) -> OneToMany | FnContext:
    for decorator in decorators:
        fn = decorator(fn)

    return FnContext(fn, **context) if context else fn


@overload
def compose_one(
    fn: OneToOne,
    *decorators: Callable[[OneToMany], OneToMany],
    context: None = None,
) -> OneToOne:
    ...


@overload
def compose_one(
    fn: OneToOne, *decorators: Callable[[OneToMany], OneToMany], context: dict
) -> FnContextOne:
    ...


def compose_one(
    fn: OneToOne,
    *decorators: Callable[[OneToMany], OneToMany],
    context: Optional[dict] = None,
) -> OneToOne | FnContextOne:
    fn = into_one(compose(into_many(fn), *decorators))

    return FnContextOne(fn, **context) if context else fn


def get_popped_fields(obj: Callable | FnContextBase) -> Optional[list[str]]:
    fn = obj.fn if isinstance(obj, FnContextBase) else obj
    return getattr(fn, POPPED_FIELDS_ATTR, None)


__all__ = [
    "OneToOne",
    "OneToMany",
    "into_many",
    "into_one",
    "pop_id_fields",
    "rename_input",
    "rename_output",
    "add_to_input",
    "add_to_output",
    "compose",
    "compose_one",
    "FnContext",
    "FnContextOne",
    "FnContextBase",
    "get_popped_fields",
]
