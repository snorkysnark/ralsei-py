import pytest
from ralsei import (
    App,
    Pipeline,
    MapToNewTable,
    Table,
    Resolves,
    ValueColumn,
    MapToNewColumns,
    PseudoTask,
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


def test_graph(app: App):
    with app.init_context() as init:
        dag = Pipeline(
            lambda root: {
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
                    table=root.outputof("aa"),
                    select="SELECT id, aa, bb FROM {{table}}",
                    columns=[ValueColumn("description", "TEXT")],
                    fn=compose_one(create_description, pop_id_fields("id")),
                ),
                "sum": AddColumnsSql(
                    table=root.outputof("aa"),
                    columns=[Column("sum", "INT")],
                    sql="""\
                    UPDATE {{table}}
                    SET sum = aa + LENGTH(bb)""",
                ),
                "aa_final": PseudoTask(
                    [
                        root.outputof("description"),
                        root.outputof("sum"),
                    ]
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
                        "source": root.outputof("aa_final"),
                        "extras": root.outputof("extras"),
                    },
                ),
            }
        ).build_dag(init)

    assert set(dag.tasks_str()) == {
        "aa",
        "description",
        "sum",
        "aa_final",
        "grouped",
        "extras",
    }
    assert dag.relations_str() == {
        "aa": {"description", "sum"},
        "description": {"aa_final"},
        "sum": {"aa_final"},
        "aa_final": {"grouped"},
        "extras": {"grouped"},
    }


def test_recursion(app: App):
    with app.init_context() as init, pytest.raises(CyclicGraphError):
        Pipeline(
            lambda root: {
                "task_a": CreateTableSql(
                    table=Table("table_a"),
                    sql="""\
                    CREATE TABLE {{table}}(
                        field REFERENCES {{other}}
                    )""",
                    params={"other": root.outputof("task_b")},
                ),
                "task_b": CreateTableSql(
                    table=Table("table_b"),
                    sql="""\
                    CREATE TABLE {{table}}(
                        field REFERENCES {{other}}
                    )""",
                    params={"other": root.outputof("task_a")},
                ),
            }
        ).build_dag(init)


def create_child_pipeline(source_tables: list[Resolves[Table]]):
    return Pipeline(
        lambda root: {
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
                params={"sources": source_tables},
            ),
            "extend": MapToNewColumns(
                table=root.outputof("join"),
                select="SELECT id, value AS aa FROM {{table}}",
                columns=[ValueColumn("description", "TEXT")],
                fn=compose_one(
                    create_description, pop_id_fields("id"), add_to_input(bb="TEXT")
                ),
            ),
        }
    )


nested_pipeline = Pipeline(
    lambda outer: {
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
        "child": create_child_pipeline([outer.outputof("aa"), outer.outputof("bb")]),
    }
)


def test_graph_nested(app: App):
    with app.init_context() as init:
        dag = nested_pipeline.build_dag(init)

    assert set(dag.tasks_str()) == {"aa", "bb", "child.join", "child.extend"}
    assert dag.relations_str() == {
        "aa": {"child.join"},
        "bb": {"child.join"},
        "child.join": {"child.extend"},
    }


def test_topological_sort(app: App):
    with app.init_context() as init:
        sorted = [
            str(named_task.name)
            for named_task in nested_pipeline.build_dag(init).topological_sort().steps
        ]

    assert sorted == ["aa", "bb", "child.join", "child.extend"] or sorted == [
        "bb",
        "aa",
        "child.join",
        "child.extend",
    ]
