from ralsei import Task
from ralsei.pipeline import resolve


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
    resolved = resolve("full", pipeline)
    resolved_names = list(map(lambda task: task.name, resolved))

    assert resolved_names == ["task_a", "task_b", "task_c"]
