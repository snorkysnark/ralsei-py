from abc import ABC, abstractmethod


class AsStatements(ABC):
    @abstractmethod
    def as_statements(self) -> list[str]: ...


def as_statements(value: object) -> list[str]:
    if isinstance(value, AsStatements):
        return value.as_statements()
    elif isinstance(value, list):
        return [str(s) for s in value]
    else:
        return [str(value)]


__all__ = ["AsStatements", "as_statements"]
