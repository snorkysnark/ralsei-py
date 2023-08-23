from typing import Callable, Generator


OneToOne = Callable[..., dict]
"""
Any function of type (*args) -> dict
"""


OneToMany = Callable[..., Generator[dict, None, None]]
"""
Any function of type (*args) -> Generator[dict]
"""
