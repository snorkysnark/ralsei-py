import pytest
from ralsei import (
    ConnectionContext,
    Pipeline,
    MapToNewTable,
    Table,
    OutputOf,
    ValueColumn,
    MapToNewColumns,
    compose_one,
    pop_id_fields,
    add_to_input,
    AddColumnsSql,
    Column,
    CreateTableSql,
    CyclicGraphError,
)


def example_data():
    yield {"aa": 8, "bb": "nyaa!!!"}
    yield {"aa": 25, "bb": "gay"}
    yield {"aa": -8, "bb": "whatever"}
    yield {"aa": 0, "bb": "ipsum"}


def create_description(aa: int, bb: str):
    return {"description": f"{bb} {{{aa}}}"}


class TestPipeline(Pipeline):
    def create_tasks(self):
        return {
            "aa": MapToNewTable(
                table=Table("aa"),
                columns=[
                    "id INTEGER PRIMARY KEY AUTOINCREMENT",
                    ValueColumn("aa", "INT"),
                    ValueColumn("bb", "TEXT"),
                ],
                fn=example_data,
            ),
            "description": MapToNewColumns(
                table=self.outputof("aa"),
                select="SELECT id, aa, bb FROM {{table}}",
                columns=[ValueColumn("description", "TEXT")],
                fn=compose_one(create_description, pop_id_fields("id")),
            ),
            "sum": AddColumnsSql(
                table=self.outputof("aa"),
                columns=[Column("sum", "INT")],
                sql="""\
                    UPDATE {{table}}
                    SET sum = aa + LENGTH(bb)""",
            ),
            "extras": CreateTableSql(
                table=Table("extras"), sql="CREATE TABLE {{table}}()"
            ),
            "grouped": CreateTableSql(
                table=Table("grouped"),
                sql="""\
                    CREATE TABLE {{table}}(
                        len INT, 
                        aa INT,
                        bb TEXT,
                        description TEXT
                    );
                    {%-split-%}

                    INSERT INTO {{table}}
                    SELECT sum, aa, bb, description FROM {{source}}
                    GROUP BY sum;
                    {%-split-%}

                    INSERT INTO {{table}}(description)
                    VALUES ({{ extras.name }});""",
                params={
                    "source": self.outputof("description", "sum"),
                    "extras": self.outputof("extras"),
                },
            ),
        }


def test_graph(ctx: ConnectionContext):
    dag = TestPipeline().build_dag(ctx.jinja)

    assert set(dag.tasks_str()) == {"aa", "description", "sum", "grouped", "extras"}
    assert dag.relations_str() == {
        "aa": {"description", "sum"},
        "description": {"grouped"},
        "sum": {"grouped"},
        "extras": {"grouped"},
    }


class ChildPipeline(Pipeline):
    def __init__(self, source_tables: list[Table | OutputOf]) -> None:
        self.source_tables = source_tables

    def create_tasks(self):
        return {
            "join": CreateTableSql(
                table=Table("joined"),
                sql="""\
                    CREATE TABLE {{table}}(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        value TEXT
                    );
                    {%-split-%}
                    {%-set sep = joiner('\nUNION\n')-%}
                    INSERT INTO {{table}}
                    {%for source in sources-%}
                    SELECT value FROM {{source}}
                    {%-endfor%}
                    """,
                params={"sources": self.source_tables},
            ),
            "extend": MapToNewColumns(
                table=self.outputof("join"),
                select="SELECT id, value AS aa FROM {{table}}",
                columns=[ValueColumn("description", "TEXT")],
                fn=compose_one(
                    create_description, pop_id_fields("id"), add_to_input(bb="TEXT")
                ),
            ),
        }


class RootPipeline(Pipeline):
    def create_tasks(self):
        return {
            "aa": CreateTableSql(
                table=Table("aa"),
                sql="""\
                    CREATE TABLE {{table}}(
                        value TEXT
                    );
                    {%-split-%}
                    INSERT INTO {{table}}
                    VALUES ('foo');
                    """,
            ),
            "bb": CreateTableSql(
                table=Table("bb"),
                sql="""\
                    CREATE TABLE {{table}}(
                        value TEXT
                    );
                    {%-split-%}
                    INSERT INTO {{table}}
                    VALUES ('bar');
                    """,
            ),
            "child": ChildPipeline([self.outputof("aa"), self.outputof("bb")]),
        }


def test_graph_nested(ctx: ConnectionContext):
    dag = RootPipeline().build_dag(ctx.jinja)

    assert set(dag.tasks_str()) == {"aa", "bb", "child.join", "child.extend"}
    assert dag.relations_str() == {
        "aa": {"child.join"},
        "bb": {"child.join"},
        "child.join": {"child.extend"},
    }


class RecursivePipeline(Pipeline):
    def create_tasks(self):
        return {
            "task_a": CreateTableSql(
                table=Table("table_a"),
                sql="""\
                CREATE TABLE {{table}}(
                    field REFERENCES {{other}}
                )""",
                params={"other": self.outputof("task_b")},
            ),
            "task_b": CreateTableSql(
                table=Table("table_b"),
                sql="""\
                CREATE TABLE {{table}}(
                    field REFERENCES {{other}}
                )""",
                params={"other": self.outputof("task_a")},
            ),
        }


def test_recursion(ctx: ConnectionContext):
    with pytest.raises(CyclicGraphError):
        RecursivePipeline().build_dag(ctx.jinja)


def test_topological_sort(ctx: ConnectionContext):
    sorted = [
        named_task.name
        for named_task in RootPipeline().build_dag(ctx.jinja).topological_sort().steps
    ]

    assert sorted == ["aa", "bb", "child.join", "child.extend"] or sorted == [
        "bb",
        "aa",
        "child.join",
        "child.extend",
    ]
