import sys
from types import SimpleNamespace
from typing import Iterable, Mapping


class TypedNamespace[T](SimpleNamespace):
    # As __orig_class__ is automatically set in generic classes, exclude it from __dict__
    __slots__ = ["__orig_class__"]

    if sys.version_info >= (3, 13):

        def __init__(
            self,
            mapping_or_iterable: Mapping[str, T] | Iterable[tuple[str, T]] = (),
            /,
            **kwargs: T,
        ) -> None:
            super().__init__(mapping_or_iterable, **kwargs)

    else:

        def __init__(self, **kwargs: T) -> None:
            super().__init__(**kwargs)

    def __setattr__(self, name: str, value: T, /) -> None:
        super().__setattr__(name, value)
