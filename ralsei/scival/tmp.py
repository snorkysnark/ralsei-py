import sqlalchemy
from ralsei.task import Task, TaskGroup, TypedNamespace
from ralsei.utils import folder


class ScivalTmp(TaskGroup):
    publications = Table("publications_raw")
    sources = Table("sources")
    record_ids = Table("record_ids")
    record_authors = Table("record_authors")
    record_affiliation = Table("record_affiliation")
    categories = Table("categories")
    record_topics = Table("record_topics")
    record_metrics = Table("record_metrics")

    def __init__(self, url: sqlalchemy.URL, config: ScivalConfig):
        self.url = url

        ns = TypedNamespace[Task]()

        ns.publications = UploadCsv(
            table=self.publications,
            sources=config.publication_list(),
            index="id_record",
            read_csv_args={"na_values": ["", "-"]},
        )
        ns.publications_srg = track(
            lambda: AddColumnsSql(
                table=require(ns.publications).table,
                columns=[Column("sgr", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET sgr = REPLACE("EID", '2-s2.0-', '')""",
            )
        )
        ns.sources = track(
            lambda: CreateTableSql(
                table=self.sources,
                sql=folder().joinpath("./sql/csml_source.sql").read_text(),
                params={"records": require(ns.publications).table},
            )
        )
        ns.sources_connect = track(
            lambda: AddColumnsSql(
                table=require(ns.publications).table,
                sql=folder().joinpath("./sql/map_source_id.sql").read_text(),
                params={"sources": require(ns.sources).table},
            )
        )
        ns.record_ids = track(
            lambda: CreateTableSql(
                table=self.record_ids,
                sql=folder().joinpath("./sql/record_ids.sql").read_text(),
                params={"records": require(ns.publications_sgr).table},
            )
        )
        ns.record_authors = track(
            lambda: MapToNewTable(
                source_table=require(ns.publications).table,
                select="""\
                SELECT
                "Scopus Author Ids" AS author_ids, "Authors" AS author_names, "Scopus Author ID Corresponding Author" as corresponding_ids,
                id_record, "EID" AS eid FROM {{source}}
                WHERE "Scopus Author Ids" IS NOT NULL""",
                table=self.record_authors,
                columns=[
                    "id_record_author {{ utils.autoincrement_primary_key() }}",
                    ValueColumn("id_record", "INT"),
                    ValueColumn("eid", "TEXT"),
                    ValueColumn("auid", "TEXT"),
                    ValueColumn("author_name", "TEXT"),
                    ValueColumn("corresponding", "TEXT"),
                ],
                fn=compose(split_author, pop_id_fields("id_record", "eid")),
            )
        )
        ns.record_affiliation = track(
            lambda: MapToNewTable(
                source_table=require(ns.publications).table,
                select='SELECT "Scopus Affiliation IDs" AS afid, id_record, "EID" AS eid FROM {{source}} WHERE "Scopus Affiliation IDs" IS NOT NULL',
                table=self.record_affiliation,
                columns=[
                    "id_record_affiliation {{ utils.autoincrement_primary_key() }}",
                    ValueColumn("afid", "TEXT"),
                    ValueColumn("id_record", "INT"),
                    ValueColumn("eid", "TEXT"),
                ],
                fn=split_column("afid"),
            )
        )
        ns.record_category = track(
            lambda: MapToNewTable(
                source_table=require(ns.publications).table,
                select="""\
                {% set sep = joiner(',\\n') -%}
                SELECT
                {{ sep() }}id_record
                {%- for name in select %}{{ sep() }}{{ name | identifier }}{% endfor %}
                FROM {{source}}""",
                table=self.categories,
                columns=[
                    ValueColumn("field_name", "TEXT"),
                    ValueColumn("value_category", "TEXT"),
                    ValueColumn("type_category", "INT"),
                    ValueColumn("id_record", "INT"),
                ],
                fn=split_categories(config.category_mapping),
                params={"select": config.category_mapping.keys()},
            )
        )
        ns.record_topics = track(
            lambda: CreateTableSql(
                table=self.record_topics,
                sql=folder().joinpath("./sql/record_topics.sql").read_text(),
                params={"records": require(ns.publications).table},
            )
        )
        ns.record_metrics = track(
            lambda: CreateTableSql(
                table=self.record_metrics,
                sql=folder().joinpath("./sql/record_metrics.sql").read_text(),
                params={"records": require(ns.publications).table},
            )
        )

        super().__init__(ns, plugins=[SqlPlugin(url)])
