from ralsei.task import Task, TaskSequence
from ralsei._pipeline import resolve_name


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
    resolved = resolve_name("full", pipeline)
    assert isinstance(resolved, TaskSequence)

    resolved_names = list(map(lambda t: t[0], resolved.named_tasks))

    assert resolved_names == ["task_a", "task_b", "task_c"]
