from jinja_psycopg import JinjaPsycopg

from .templates import Column


class RalseiRenderer(JinjaPsycopg):
    def _prepare_environment(self):
        super()._prepare_environment()

        self._env.globals["Column"] = Column


DEFAULT_RENDERER = RalseiRenderer()
