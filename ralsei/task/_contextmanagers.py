class MultiContextManager:
    def __init__(self, context_managers: dict) -> None:
        self._context_managers = context_managers

    def __enter__(self) -> dict:
        return {name: obj.__enter__() for name, obj in self._context_managers.items()}

    def __exit__(self, *excinfo):
        for context_manager in self._context_managers.values():
            context_manager.__exit__(*excinfo)
