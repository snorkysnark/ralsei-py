from jinja_psycopg import JinjaPsycopg

from .templates import Column


class RalseiRenderer(JinjaPsycopg):
    """
    [JinjaPsycopg](https://github.com/snorkysnark/jinja-psycopg)
    renderer pre-initialized with ralsei's environment variables

    Better documentation coming soon

    Example:
        Build template:
        ```python
        renderer.from_string("SELECT * FROM {{table}}")
        # jinja_psycopg.SqlTemplate
        ```

    Example:
        Render SQL:
        ```python
        renderer.render("SELECT * FROM {{table}}", {"table": Table("foo")})
        # psycopg.sql.Composed
        ```
    """

    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Column"] = Column


DEFAULT_RENDERER = RalseiRenderer()
