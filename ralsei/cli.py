import argparse
from argparse import ArgumentParser
from typing import Callable, Optional
import sqlalchemy

from ralsei._pipeline import Pipeline, TaskDefinitions
from ralsei.connection import PsycopgConn, create_connection_url


class RalseiCli:
    def __init__(self) -> None:
        """
        Command line interface for running ralsei pipelines
        """

        parser = ArgumentParser()
        parser.add_argument("task", help="Task name in the pipeline")
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("--db", help="postgres:// url or .json file")
        self._argparser = parser

        custom_group = parser.add_argument_group("custom", "Custom arguments")

        # Allows for better intellisense than using a wrapper method
        self.add_argument = custom_group.add_argument
        """
        Mimics [argparse.ArgumentParser.add_argument][]

        ```
        add_argument(
            name or flags...
            [, action][, nargs][, const][, default][, type]
            [, choices][, required][, help][, metavar][, dest]
        )
        ```
        """

    def run(
        self,
        task_tree: TaskDefinitions | Callable[[argparse.Namespace], TaskDefinitions],
        credentials: Optional[str] = None,
    ):
        """
        Parse arguments and run the corresponding tasks

        Args:
            task_tree: a dictionary declaring the tasks
                or a function that receives the cli arguments and creates said dictionary
            credentials: either `postgres://` url or a path to json file
        """

        args = self._argparser.parse_args()

        credentials = args.db or credentials
        if not credentials:
            raise ValueError("credentials not specified")

        if isinstance(task_tree, Callable):
            task_tree = task_tree(args)

        pipeline = Pipeline(task_tree)
        task = pipeline[args.task]

        engine = sqlalchemy.create_engine(create_connection_url(credentials))
        with engine.connect() as sqlalchemy_conn:
            conn = PsycopgConn(sqlalchemy_conn)

            if args.action == "run":
                task.run(conn)
            elif args.action == "delete":
                task.delete(conn)
            elif args.action == "redo":
                task.delete(conn)
                task.run(conn)
            elif args.action == "describe":
                task.describe(conn)


__all__ = ["RalseiCli"]
