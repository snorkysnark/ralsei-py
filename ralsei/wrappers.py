from typing import Any, Callable, Iterator, TypeVar
from functools import wraps

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
        metadata = getattr(wrapper, "id_fields", [])
        metadata.extend(id_fields)
        setattr(wrapper, "id_fields", metadata)

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


def compose(fn: T, *decorators: Callable[[T], T]) -> T:
    for decorator in decorators:
        fn = decorator(fn)

    return fn


def compose_one(
    fn: OneToOne, *decorators: Callable[[OneToMany], OneToMany]
) -> OneToOne:
    return into_one(compose(into_many(fn), *decorators))


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
]
