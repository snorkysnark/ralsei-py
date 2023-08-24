from jinja_psycopg import JinjaPsycopg

from .templates import Column


class RalseiRenderer(JinjaPsycopg):
    """
    JinjaPsycopg renderer pre-initialized with ralsei's environment variables

    ## Usage

    Build template:
    ```python
    renderer.from_string("SELECT * FROM {{table}}")
    # jinja_psycopg.SqlTemplate
    ```

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
