from __future__ import annotations

import sqlfmt.api
from sqlfmt.mode import Mode

DEFAULT_MODE = Mode()


def format_sql(sql: str) -> str:
    return sqlfmt.api.format_string(sql, DEFAULT_MODE).strip()
