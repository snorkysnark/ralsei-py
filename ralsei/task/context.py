from contextvars import ContextVar

ROW_CONTEXT_VAR: ContextVar[dict] = ContextVar("ROW_CONTEXT")
ROW_CONTEXT_ATRRIBUTE = "__ralsei_row_context"


class RowContext:
    def __init__(self, popped_fields: dict) -> None:
        self._popped_fields = popped_fields
        self._token = None

    @staticmethod
    def from_input_row(input_row: dict, popped_names: set[str]) -> "RowContext":
        return RowContext(
            {key: value for key, value in input_row.items() if key in popped_names}
        )

    def __enter__(self) -> "RowContext":
        self._token = ROW_CONTEXT_VAR.set(self._popped_fields)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._token:
            ROW_CONTEXT_VAR.reset(self._token)

        if exc_value:
            setattr(exc_value, ROW_CONTEXT_ATRRIBUTE, self._popped_fields)


__all__ = ["ROW_CONTEXT_ATRRIBUTE", "ROW_CONTEXT_VAR", "RowContext"]
