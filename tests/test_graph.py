from ralsei import (
    ConnectionContext,
    Pipeline,
    MapToNewTable,
    Table,
    ValueColumn,
    MapToNewColumns,
    OutputOf,
    compose_one,
    pop_id_fields,
    AddColumnsSql,
    Column,
    CreateTableSql,
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
                table=OutputOf("aa"),
                select="SELECT id, aa, bb FROM {{table}}",
                columns=[ValueColumn("description", "TEXT")],
                fn=compose_one(create_description, pop_id_fields("id")),
            ),
            "sum": AddColumnsSql(
                table=OutputOf("aa"),
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
                    "source": OutputOf("description") + OutputOf("sum"),
                    "extras": OutputOf("extras"),
                },
            ),
        }


def test_graph(ctx: ConnectionContext):
    graph = TestPipeline().graph(ctx.jinja)

    assert set(graph.tasks_by_name) == {"aa", "description", "sum", "grouped", "extras"}
    assert dict(graph.relations) == {
        "aa": {"description", "sum"},
        "description": {"grouped"},
        "sum": {"grouped"},
        "extras": {"grouped"},
    }
