from typing import Callable, Generator


# Any function of type (*args) -> dict
OneToOne = Callable[..., dict]


# Any function of type (*args) -> Generator[dict]
OneToMany = Callable[..., Generator[dict, None, None]]
