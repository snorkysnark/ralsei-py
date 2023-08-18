import argparse
from argparse import ArgumentParser
from typing import Callable, Optional, Union
from pathlib import Path
import sqlalchemy
import json

from ralsei.pipeline import Pipeline
from ralsei.pipeline.pipeline import TaskDefinitions
from ralsei.context import PsycopgConn


def create_connection_url(credentials: str) -> sqlalchemy.URL:
    if credentials.endswith(".json"):
        with Path(credentials).open() as file:
            creds_dict = json.load(file)
            creds_dict["drivername"] = "postgresql+psycopg"
            return sqlalchemy.URL.create(**creds_dict)
    else:
        url = sqlalchemy.make_url(credentials)
        return sqlalchemy.URL.create(
            "postgresql+psycopg",
            url.username,
            url.password,
            url.host,
            url.port,
            url.database,
        )


TaskDefinitionsFactory = Callable[[argparse.Namespace], TaskDefinitions]


class RalseiCli:
    def __init__(self) -> None:
        parser = ArgumentParser()
        parser.add_argument("task", help="Task name in the pipeline")
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("--db", help="postgres:// url or .json file")
        self._argparser = parser

        # Allows for better intellisense than using a wrapper method
        custom_group = parser.add_argument_group("custom", "Custom arguments")
        self.add_argument = custom_group.add_argument

    def run(
        self,
        task_tree: Union[TaskDefinitions, TaskDefinitionsFactory],
        credentials: Optional[str] = None,
    ):
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
