from abc import ABC, abstractmethod
from typing import Any

from .protocols import OneToOne, OneToMany


class FnWrapper(ABC):
    """An abstract function wrapper factory"""

    @abstractmethod
    def wrap(self, fn: OneToMany) -> OneToMany:
        """
        Apply a wrapper to fn and return the wrapped function

        Args:
        - fn ((*args) -> Generator[dict]): inner function
        """
        pass


class PopIdFields(FnWrapper):
    """(Optionally) removes fields from the input kwargs
    and reinserts them back into the output"""

    def __init__(self, *id_fields: str, keep: bool = False) -> None:
        """Args:
        - `*id_fields` (str): fields to pop from the kwargs
        - keep (bool, optional): if True, `id_fields` are not removed from kwargs
        (but still remembered and reinserted into the output), False by default"""
        self._id_fields = id_fields
        self._keep = keep

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            id_field_values = {}

            # Pop (or copy) id fields from input_row and save them in id_field_values
            for id_field in self._id_fields:
                id_field_values[id_field] = (
                    input_row[id_field] if self._keep else input_row.pop(id_field)
                )

            # Reinsert id_field_values into output
            for output_row in fn(**input_row):
                yield {**id_field_values, **output_row}

        return wrapper


class RenameInput(FnWrapper):
    """Remaps field names in the `**kwargs` dictionary"""

    def __init__(self, remap_fields: dict[str, str]) -> None:
        self._remap_fields = remap_fields

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            new_input_row = {}

            for old_name, value in input_row.items():
                new_name = self._remap_fields.get(old_name, old_name)
                new_input_row[new_name] = value

            yield from fn(**new_input_row)

        return wrapper


class RenameOutput(FnWrapper):
    """Remaps field values in dictionaries generated by the inner function"""

    def __init__(self, remap_fields: dict[str, str]) -> None:
        self._remap_fields = remap_fields

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            for output_row in fn(**input_row):
                new_output_row = {}

                for old_name, value in output_row.items():
                    new_name = self._remap_fields.get(old_name, old_name)
                    new_output_row[new_name] = value

                yield new_output_row

        return wrapper


class AddToInput(FnWrapper):
    """Add extra key-value pairs to input"""

    def __init__(self, add_values: dict[str, Any]) -> None:
        self._add_values = add_values

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            yield from fn(**input_row, **self._add_values)

        return wrapper


class AddToOutput(FnWrapper):
    """Add extra key-value pairs to output"""

    def __init__(self, add_values: dict[str, Any]) -> None:
        self._add_values = add_values

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            for output_row in fn(**input_row):
                yield {**output_row, **self._add_values}

        return wrapper


def into_many(fn: OneToOne) -> OneToMany:
    """Convert a regular function
    into a generator function yielding a single value"""

    def wrapper(**input_row: Any):
        yield fn(**input_row)

    return wrapper


def into_one(fn: OneToMany) -> OneToOne:
    """Convert an function that returns a generator yielding a single value
    into a function that returns that value"""

    def wrapper(**input_row: Any):
        generator = fn(**input_row)
        first_value = next(generator)

        # If there's more than one value in the generator, throw an error
        try:
            next(generator)
        except StopIteration:
            return first_value

        raise ValueError("Passed generator that returns more than one value")

    return wrapper
