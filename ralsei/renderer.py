from jinja_psycopg.renderer import JinjaPsycopg

from .templates import Column


class RalseiRenderer(JinjaPsycopg):
    """
    Jinja+Psycopg
    renderer pre-initialized with ralsei's environment variables

    See [JinjaPsycopg](https://snorkysnark.github.io/jinja-psycopg/)'s page for usage
    """

    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Column"] = Column


DEFAULT_RENDERER = RalseiRenderer()
