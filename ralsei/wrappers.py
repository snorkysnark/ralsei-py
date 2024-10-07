from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
)
from functools import wraps

POPPED_FIELDS_ATTR = "__ralsei_popped_fields"

type OneToOne = Callable[..., dict[str, Any]]
"""Row to row mapping function

.. code-block:: python

    def example(html: str):
        return {"name": get_name(html)}
"""
type OneToMany = Callable[..., Iterator[dict[str, Any]]]
"""One row to many rows mapping function

.. code-block:: python

    def example(html: str):
        for name in get_names(html):
            yield {"name": name}
"""


def into_many(fn: OneToOne) -> OneToMany:
    """Turn :py:type:`~OneToOne` mapping function into :py:type:`~OneToMany`"""

    @wraps(fn)
    def wrapper(**kwargs: Any):
        yield fn(**kwargs)

    return wrapper


def into_one(fn: OneToMany) -> OneToOne:
    """Turn :py:type:`~OneToMany` mapping function into :py:type:`~OneToOne`

    Would throw an error if the input function yields more than one row
    """

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


def pop_id_fields(
    *id_fields: str, keep: bool = False
) -> Callable[[OneToMany], OneToMany]:
    """Create function wrapper that 'pops' ``id_fields`` off the keyword arguments,
    calls the inner function without them, then re-inserts them into the output rows

    .. code-block:: pycon

        >>> @pop_id_fields("id")
        ... def foo(a: int):
        ...     yield {"b": a * 2}
        ...
        >>> next(foo(id=5, a=3))
        {"id": 5, "b": 6}

    Additionally, popped field names are saved into the function's metadata,
    so that tasks can use them for inferring :py:class:`IdColumns <ralsei.types.IdColumn>`

    Args:
        id_fields: keyword arguments to pop
        keep: if ``True``, the popped arguments would **still** be passed to the inner function

            .. code-block:: pycon

                >>> @pop_id_fields("year")
                ... def foo(year: int, name: str):
                ...     yield {"html": download(year, name) }
                ...
                >>> next(foo(year=2015, name="Tokyo"))
                {"year": 2015, "json": {...}}
    """

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


def rename_input(**mapping: str) -> Callable[[OneToMany], OneToMany]:
    """Create function wrapper that remaps keyword argument names

    .. code-block:: python

        @rename_input(a="b")
        def foo(b: int):
            yield {...}

        foo(a=10)

    """

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            yield from fn(
                **{mapping.get(key, key): value for key, value in kwargs.items()}
            )

        return wrapper

    return decorator


def rename_output(**mapping: str) -> Callable[[OneToMany], OneToMany]:
    """Create function wrapper that remaps fields in the output dictionary

    .. code-block:: pycon

        >>> @rename_output(a="b")
        ... def foo():
        ...     yield {"a": 5}
        ...
        >>> next(foo())
        {"b": 5}
    """

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            for row in fn(**kwargs):
                yield {mapping.get(key, key): value for key, value in row.items()}

        return wrapper

    return decorator


def add_to_input(**add_values: Any) -> Callable[[OneToMany], OneToMany]:
    """Create function wrapper that adds to the keyword arguments

    .. code-block:: python

        @add_to_input(b="meow")
        def foo(a: int, b: str):
            yield {...}

        foo(a=5)
    """

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            yield from fn(**kwargs, **add_values)

        return wrapper

    return decorator


def add_to_output(**add_values: Any) -> Callable[[OneToMany], OneToMany]:
    """Create function wrapper that adds entries to the output dictionary

    .. code-block:: pycon

        >>> @add_to_output(b="meow")
        ... def foo():
        ...     yield {"a": 10}
        ...
        >>> next(foo())
        {"a": 10, "b": "meow"}
    """

    def decorator(fn: OneToMany) -> OneToMany:
        @wraps(fn)
        def wrapper(**kwargs):
            for row in fn(**kwargs):
                yield {**row, **add_values}

        return wrapper

    return decorator


def compose(fn: OneToMany, *decorators: Callable[[OneToMany], OneToMany]) -> OneToMany:
    """Compose multiple decorators together on a :py:type:`~OneToMany`

    Args:
        fn: base function
        decorators: decorators to apply
    """

    for decorator in decorators:
        fn = decorator(fn)

    return fn


def compose_one(
    fn: OneToOne,
    *decorators: Callable[[OneToMany], OneToMany],
) -> OneToOne:
    """Compose multiple decorators together on a :py:type:`~OneToOne`

    Args:
        fn: base function
        decorators: decorators to apply
    """

    return into_one(compose(into_many(fn), *decorators))


def get_popped_fields(fn: Callable) -> Optional[list[str]]:
    """Get fields popped by :py:func:`~pop_id_fields` from the function metadata"""
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
    "get_popped_fields",
]
