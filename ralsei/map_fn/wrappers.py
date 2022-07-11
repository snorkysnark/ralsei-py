from abc import ABC, abstractmethod
from typing import Any

from .protocols import OneToOne, OneToMany


class FnWrapper(ABC):
    @abstractmethod
    def wrap(self, fn: OneToMany) -> OneToMany:
        pass


class PopIdFields(FnWrapper):
    def __init__(self, id_fields: list[str]) -> None:
        self.id_fields = id_fields

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            id_field_values = {}

            for id_field in self.id_fields:
                id_field_values[id_field] = input_row.pop(id_field)

            for output_row in fn(**input_row):
                yield {**id_field_values, **output_row}

        return wrapper


class RenameInput(FnWrapper):
    def __init__(self, remap_fields: dict[str, str]) -> None:
        self.remap_fields = remap_fields

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):
            new_input_row = {}

            for old_name, value in input_row.items():
                new_name = self.remap_fields.get(old_name, old_name)
                new_input_row[new_name] = value

            yield from fn(**input_row)

        return wrapper


class RenameOutput(FnWrapper):
    def __init__(self, remap_fields: dict[str, str]) -> None:
        self.remap_fields = remap_fields

    def wrap(self, fn: OneToMany) -> OneToMany:
        def wrapper(**input_row: Any):

            for output_row in fn(**input_row):
                new_output_row = {}

                for old_name, value in output_row.items():
                    new_name = self.remap_fields.get(old_name, old_name)
                    new_output_row[new_name] = value

                yield new_output_row

        return wrapper


def into_many(fn: OneToOne) -> OneToMany:
    def wrapper(**input_row: Any):
        yield fn(**input_row)

    return wrapper


def into_one(fn: OneToMany) -> OneToOne:
    def wrapper(**input_row: Any):
        generator = fn(**input_row)
        first_value = next(generator)

        try:
            next(generator)
        except StopIteration:
            return first_value

        raise ValueError("Passed generator that returns more than one value")

    return wrapper
