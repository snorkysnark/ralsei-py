import argparse
from argparse import ArgumentParser
from typing import Callable, Optional, Sequence, Union
from pathlib import Path
from rich.console import Console
from rich.syntax import Syntax
import sqlalchemy
import json
import sys

import ralsei.pipeline
from ralsei.pipeline import NamedTask, Pipeline
from ralsei.runner import TaskRunner
from ralsei.context import PsycopgConn
from ralsei.task import Task
from ralsei.templates.renderer import RalseiRenderer

PipelineFactory = Callable[[argparse.Namespace], Pipeline]


def create_connection_url(credentials: str) -> sqlalchemy.URL:
    if credentials.endswith(".json"):
        with Path(credentials).open() as file:
            creds_dict = json.load(file)
            creds_dict["drivername"] = "postgresql+psycopg"
            return sqlalchemy.URL.create(**creds_dict)
    elif credentials.find("://") != -1:
        url = sqlalchemy.make_url(credentials)
        return sqlalchemy.URL.create(
            "postgresql+psycopg",
            url.username,
            url.password,
            url.host,
            url.port,
            url.database,
        )
    else:
        return sqlalchemy.make_url("postgresql+psycopg://" + credentials)


def render_task_scripts(conn: PsycopgConn, task: Task):
    return "\n\n".join(
        map(
            lambda item: f"-- {item[0]}\n{item[1].as_string(conn.pg())}",
            task.scripts.items(),
        )
    )


def describe_task(conn: PsycopgConn, task: Task):
    sql = render_task_scripts(conn, task)

    if sys.stdout.isatty():
        Console().print(Syntax(sql, "sql"))
    else:
        print(sql)


def describe_sequence(conn: PsycopgConn, task_sequence: Sequence[NamedTask]):
    if len(task_sequence) == 1:
        named_task = task_sequence[0]
        describe_task(conn, named_task.task)
    else:
        for named_task in task_sequence:
            print(named_task.name)


class RalseiCli:
    def __init__(self) -> None:
        parser = ArgumentParser()
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("task")
        parser.add_argument("--db", help="connection url")
        self._argparser = parser

        # Allows for better intellisense than using a wrapper method
        custom_group = parser.add_argument_group("custom", "Custom arguments")
        self.add_argument = custom_group.add_argument

    def run(
        self,
        pipeline: Union[Pipeline, PipelineFactory],
        credentials: Optional[str] = None,
    ):
        args = self._argparser.parse_args()

        credentials = args.db or credentials
        if not credentials:
            raise ValueError("credentials not specified")

        if isinstance(pipeline, Callable):
            pipeline = pipeline(args)

        task_sequence = list(ralsei.pipeline.resolve(args.task, pipeline))

        renderer = RalseiRenderer()
        for named_task in task_sequence:
            named_task.task.render(renderer)

        engine = sqlalchemy.create_engine(create_connection_url(credentials))
        with engine.connect() as sqlalchemy_conn:
            conn = PsycopgConn(sqlalchemy_conn)

            runner = TaskRunner(conn)
            if args.action == "run":
                runner.run(task_sequence)
            elif args.action == "delete":
                runner.delete(task_sequence)
            elif args.action == "redo":
                runner.redo(task_sequence)
            elif args.action == "describe":
                describe_sequence(conn, task_sequence)
