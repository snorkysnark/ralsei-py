from typing import Any, Generator, Protocol


class OneToOne(Protocol):
    def __call__(self, *args: Any) -> dict:
        ...


class OneToMany(Protocol):
    def __call__(self, *args: Any) -> Generator[dict, None, None]:
        ...
