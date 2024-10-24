from functools import wraps
from types import TracebackType
from typing import Callable, Generator, Optional, Protocol
from contextlib import contextmanager, _GeneratorContextManager


class ContextManager[T](Protocol):
    """Protocol describing any context manager class"""

    def __enter__(self) -> T: ...

    def __exit__(
        self,
        __exc_type: Optional[type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> Optional[bool]: ...


class MultiContextManager[T]:
    """Makes a dictionary of context managers act as a single context manager

    .. code-block:: pycon

        >>> with MultiContextManager(
        ...     {"sess": requests.Session(), "file": open("file.txt")}
        ... ) as context:
        ...     print(context)
        {
            'sess': <requests.sessions.Session object at 0x742d72897680>,
            'file': <_io.TextIOWrapper name='file.txt' mode='r' encoding='UTF-8'>
        }

    """

    def __init__(self, context_managers: dict[str, ContextManager[T]]) -> None:
        self.context_managers = context_managers

    def __enter__(self) -> dict[str, T]:
        return {name: obj.__enter__() for name, obj in self.context_managers.items()}

    def __exit__(self, __exc_type, __exc_value, __traceback):
        for context_manager in self.context_managers.values():
            context_manager.__exit__(__exc_type, __exc_value, __traceback)


class _ReusableGeneratorContextManager[T, **P]:
    def __init__(
        self,
        make_contextmanager: Callable[P, _GeneratorContextManager[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.make_contextmanager = make_contextmanager
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        self.oneshot = self.make_contextmanager(*self.args, **self.kwargs)
        return self.oneshot.__enter__()

    def __exit__(self, __exc_type, __exc_value, __traceback):
        result = self.oneshot.__exit__(__exc_type, __exc_value, __traceback)
        del self.oneshot

        return result


def reusable_contextmanager[
    T, **P
](func: Callable[P, Generator[T, None, None]]) -> Callable[
    P, _ReusableGeneratorContextManager[T, P]
]:
    """like :py:func:`contextlib.contextmanager`, but can be entered multiple times

    .. code-block:: python

        @reusable_contextmanager
        def foo(value: int):
            yield value

        ctx = foo(1)

        with ctx as value:
            print(value) # Prints 1
        with ctx as value:
            print(value) # Also prints 1
    """

    wrapped = contextmanager(func)

    @wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs):
        return _ReusableGeneratorContextManager(wrapped, *args, **kwargs)

    return inner


def reusable_contextmanager_const[
    T
](func: Callable[[], Generator[T, None, None]]) -> _ReusableGeneratorContextManager[
    T, []
]:
    """Like :py:func:`ralsei.contextmanagers.reusable_contextmanager`, but used without invocation

    Only for functions with no arguments.

    .. code-block:: python

        @reusable_contextmanager_const
        def foo():
            yield 1

        with foo as value:
            print(value)
    """

    return _ReusableGeneratorContextManager(contextmanager(func))


__all__ = [
    "ContextManager",
    "MultiContextManager",
    "reusable_contextmanager",
    "reusable_contextmanager_const",
]
