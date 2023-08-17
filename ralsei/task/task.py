from abc import ABC, abstractmethod
from psycopg.sql import Composed

from ralsei.context import PsycopgConn
from ralsei.templates.renderer import RalseiRenderer


class Task(ABC):
    def __init__(self) -> None:
        self.scripts: dict[str, Composed] = {}

    @abstractmethod
    def run(self, conn: PsycopgConn, renderer: RalseiRenderer) -> None:
        """Execute the task"""

    @abstractmethod
    def delete(self, conn: PsycopgConn, renderer: RalseiRenderer) -> None:
        """Delete whatever `run()` method has created"""

    @abstractmethod
    def render(self, renderer: RalseiRenderer) -> None:
        """
        Render your sql scripts here, like this:

        `self.scripts["Create table"] = self.__create_table = renderer.render(...)`
        """
