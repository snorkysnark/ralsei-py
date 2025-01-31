import sqlalchemy
from ralsei.task import Task, TaskGroup, TypedNamespace
from ralsei.utils import folder

from .tmp import ScivalTmp


def _make_scival_main(url: sqlalchemy.URL, tmp: ScivalTmp):
    ns = TypedNamespace[Task]()

    ns.csml_source = CreateTableSql(
        table=Table("csml_source"),
        sql=folder().joinpath("./sql/csml_source.sql").read_text(),
        params={"tmp": tmp.sources, "slice": slice},
    )
    ns.type_database_record = CreateTableSql(
        table=Table("csml_type_database_record"),
        sql=folder().joinpath("./sql/type_database_record.sql").read_text(),
    )
    ns.csml_record = track(
        CreateTableSql(
            table=Table("csml_record"),
            sql=folder().joinpath("./sql/csml_record.sql").read_text(),
            params={
                "type_database_record": require(ns.type_database_record).table,
                "csml_source": require(ns.csml_source).table,
                "tmp": tmp.publications,
                "slice": slice,
            },
        )
    )
    ns.csml_record_affiliation = track(
        lambda: CreateTableSql(
            table=Table("csml_record_affiliation"),
            sql=folder().joinpath("./sql/record_affiliation.sql").read_text(),
            params={
                "csml_record": require(ns.csml_record).table,
                "tmp": tmp.record_affiliation,
            },
        )
    )
    ns.csml_record_author = track(
        lambda: CreateTableSql(
            table=Table("csml_record_author"),
            sql=folder().joinpath("./sql/record_author.sql").read_text(),
            params={
                "csml_record": require(ns.csml_record).table,
                "tmp": tmp.record_authors,
            },
        )
    )
    ns.csml_type_category = CreateTableSql(
        table=Table("csml_type_category"),
        sql=folder().joinpath("./sql/type_category.sql").read_text(),
    )
    ns.csml_record_category = track(
        lambda: CreateTableSql(
            table=Table("csml_record_category"),
            sql=folder().joinpath("./sql/record_category.sql").read_text(),
            params={
                "csml_record": require(ns.csml_record).table,
                "csml_type_category": require(csml_type_category).table,
                "tmp": tmp.categories,
            },
        )
    )
    ns.csml_type_record_ids = CreateTableSql(
        table=Table("csml_type_record_ids"),
        sql=folder().joinpath("./sql/type_record_ids.sql").read_text(),
    )
    ns.record_ids = track(
        lambda: CreateTableSql(
            table=Table("csml_record_ids"),
            sql=folder().joinpath("./sql/record_ids.sql").read_text(),
            params={
                "csml_type_record_ids": require(ns.csml_type_record_ids),
                "csml_record": require(ns.csml_record).table,
                "tmp": tmp.record_ids,
            },
        )
    )
    ns.record_metrics = track(
        lambda: CreateTableSql(
            table=Table("csml_record_metrics"),
            sql=folder().joinpath("./sql/record_metrics.sql").read_text(),
            params={
                "csml_record": require(ns.csml_record).table,
                "tmp": tmp.record_metrics,
            },
        )
    )
    ns.record_topics = track(
        lambda: CreateTableSql(
            table=Table("csml_record_topics"),
            sql=folder().joinpath("./sql/record_topics.sql").read_text(),
            params={
                "tmp": tmp.record_topics,
                "records": require(ns.csml_record).table,
            },
        )
    )

    return TaskGroup(ns, plugins=SqlPlugin(url, attach={"tmp": tmp.url}))


def make_scival(url: str, config: ScivalConfig):
    ns = TypedNamespace[Task]()

    ns.tmp = ScivalTmp(make_tmp_url(url), config)
    ns.main = track(lambda: _make_scival_main(url, require(ns.tmp)))

    return TaskGroup(ns)
