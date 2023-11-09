from abc import abstractmethod
from psycopg.sql import Composed

from ralsei.connection import PsycopgConn


class Task:
    """
    Attributes:
        scripts: Named SQL scripts created by the `render` method
    """

    scripts: dict[str, Composed]

    def __init__(self) -> None:
        """Base Task class"""
        self.scripts = {}

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
