from typing import List

from .protocols import OneToOne, OneToMany
from .wrappers import FnWrapper, PopIdFields, RenameOutput, into_one, into_many


class FnBuilderBase:
    def __init__(self) -> None:
        self.wrappers: List[FnWrapper] = []

    def add_wrapper(self, wrapper: FnWrapper):
        self.wrappers.append(wrapper)
        return self

    def pop_id_fields(self, *id_fields: str):
        self.add_wrapper(PopIdFields([*id_fields]))
        return self

    def rename_output(self, remap_fields: dict[str, str]):
        self.add_wrapper(RenameOutput(remap_fields))
        return self

    def _wrap_all(self, chain: OneToMany) -> OneToMany:
        for wrapper in reversed(self.wrappers):
            chain = wrapper.wrap(chain)
        return chain


class GeneratorBuilder(FnBuilderBase):
    def __init__(self, fn: OneToMany) -> None:
        super().__init__()
        self.fn = fn

    def build(self) -> OneToMany:
        return self._wrap_all(self.fn)


class FnBuilder(FnBuilderBase):
    def __init__(self, fn: OneToOne) -> None:
        super().__init__()
        self.fn = fn

    def build(self) -> OneToOne:
        return into_one(self._wrap_all(into_many(self.fn)))
