from ralsei.jinjasql import JinjaSqlEngine
from ralsei.dialect import PostgresDialect, DialectRegistry


class CustomDialect(PostgresDialect):
    pass


def test_custom_dialect():
    registry = DialectRegistry.create_default()
    registry.register_dialect("postgresql", CustomDialect, driver="psycopg2")

    engine_ctx = JinjaSqlEngine.create("postgresql:///ralsei_test", dialect=registry)
    assert isinstance(engine_ctx.dialect, CustomDialect)
