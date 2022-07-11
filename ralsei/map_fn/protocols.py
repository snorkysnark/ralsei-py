from typing import Any, Generator, Protocol


class OneToOne(Protocol):
    """Any function of type (*args) -> dict"""
    def __call__(self, *args: Any) -> dict:
        ...


class OneToMany(Protocol):
    """Any function of type (*args) -> Generator[dict]"""
    def __call__(self, *args: Any) -> Generator[dict, None, None]:
        ...
