from argparse import ArgumentParser
from typing import Callable, Optional
import sqlalchemy

from ralsei._pipeline import Pipeline, TaskDefinitions
from ralsei.connection import PsycopgConn, create_connection_url
from ralsei.task import Task


def _create_action(task_name: str, action_name: str, credentials: str):
    def action(definitions: TaskDefinitions):
        pipeline = Pipeline(definitions)
        task = pipeline[task_name]

        engine = sqlalchemy.create_engine(create_connection_url(credentials))
        with engine.connect() as sqlalchemy_conn:
            conn = PsycopgConn(sqlalchemy_conn)

            actions: dict[str, Callable[[Task], None]] = {
                "run": lambda task: task.run(conn),
                "delete": lambda task: task.delete(conn),
                "redo": lambda task: task.redo(conn),
                "describe": lambda task: task.describe(conn),
            }
            actions[action_name](task)

    return action


class RalseiCli:
    def __init__(self, default_credentials: Optional[str] = None) -> None:
        """
        Command line interface for running ralsei pipelines
        """

        parser = ArgumentParser()
        parser.add_argument("task", help="Task name in the pipeline")
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument(
            "--db", help="postgres:// url or .json file", default=default_credentials
        )
        self._argparser = parser

        custom_group = parser.add_argument_group("custom", "Custom arguments")
        self._custom_group = custom_group

    def add_argument(self, *args, **kwargs):
        if (
            not args
            or len(args) == 1
            and args[0][0] not in self._argparser.prefix_chars
        ):
            raise ValueError("Only keyword arguments are allowed")

        self._custom_group.add_argument(*args, **kwargs)

    def parse_args(self):
        args = self._argparser.parse_args()
        custom_args = {
            action.dest: getattr(args, action.dest)
            for action in self._custom_group._group_actions
        }

        return _create_action(args.task, args.action, args.db), custom_args

    def run(
        self,
        create_tasks: Callable[..., TaskDefinitions],
    ):
        action, args = self.parse_args()
        action(create_tasks(**args))


__all__ = ["RalseiCli"]
