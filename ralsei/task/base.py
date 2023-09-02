from abc import abstractmethod
from psycopg.sql import Composed

from ralsei.connection import PsycopgConn
from ralsei.renderer import RalseiRenderer


class Task:
    """
    Attributes:
        scripts: Named SQL scripts created by the `render` method
    """

    scripts: dict[str, Composed]

    def __init__(self) -> None:
        """Base Task class"""
        self.scripts = {}

    def render(self, renderer: RalseiRenderer) -> None:
        """
        Render your sql scripts here, like this:
        ```python
        self.scripts["Create table"] = self.__create_table = renderer.render(...)
        ```

        This methon runs before `run` and `delete`

        Args:
            renderer: jinja sql renderer
        """
        pass

    @abstractmethod
    def exists(self, conn: PsycopgConn) -> bool:
        """
        Check if task has already been done

        Args:
            conn: db connection

        Returns:
            has been done?
        """

    @abstractmethod
    def run(self, conn: PsycopgConn) -> None:
        """
        Execute the task

        Args:
            conn: db connection
        """

    @abstractmethod
    def delete(self, conn: PsycopgConn) -> None:
        """
        Delete whatever `run()` method has created

        Args:
            conn: db connection
        """
