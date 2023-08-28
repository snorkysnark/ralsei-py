from typing import Callable, Iterator


OneToOne = Callable[..., dict]
"""
A function that maps one row to multiple rows
"""


OneToMany = Callable[..., Iterator[dict]]
"""
A function that maps one row to another
"""
