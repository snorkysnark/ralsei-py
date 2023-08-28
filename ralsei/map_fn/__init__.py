"""
Utilities for building wrappers around a function, such as:

- renaming an key in `**kwargs`
- renaming a key in the returned/yielded dictionaries
- popping a key from input and re-attaching it to the output
- adding arguments to the input, etc..

You'll most likely be using [ralsei.map_fn.FnBuilder][] for simple functions
and [ralsei.map_fn.GeneratorBuilder][] for generator functions
"""

from .protocols import *
from .wrappers import FnWrapper as FnWrapper
from .builders import *
