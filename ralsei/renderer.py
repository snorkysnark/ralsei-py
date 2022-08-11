from jinja_psycopg import JinjaPsycopg


class RalseiRenderer(JinjaPsycopg):
    pass


DEFAULT_RENDERER = RalseiRenderer()
