from abc import ABC, abstractmethod

from ralsei.context import ConnectionContext


class Runnable(ABC):
    @abstractmethod
    def run(self, ctx: ConnectionContext):
        ...

    @abstractmethod
    def delete(self, ctx: ConnectionContext):
        ...

    def redo(self, ctx: ConnectionContext):
        self.delete(ctx)
        self.run(ctx)
