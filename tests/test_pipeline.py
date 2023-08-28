from ralsei.task import Task
from ralsei._pipeline import Sequence, resolve_name
from ralsei.renderer import RalseiRenderer


class DummyTask(Task):
    def run(self, conn, env):
        pass

    def delete(self, conn, env):
        pass


def test_resolve():
    pipeline = {
        "task_a": DummyTask(),
        "task_b": DummyTask(),
        "task_c": DummyTask(),
        "group_ab": [
            "task_a",
            "task_b",
        ],
        "full": ["group_ab", "task_c"],
    }
    resolved = resolve_name("full", pipeline, RalseiRenderer())
    assert isinstance(resolved, Sequence)

    resolved_names = list(map(lambda task: task.name, resolved.tasks))

    assert resolved_names == ["task_a", "task_b", "task_c"]
