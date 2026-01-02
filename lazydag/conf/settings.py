from pathlib import Path
from importlib import import_module
from types import ModuleType
import os
from typing import Any


class LazySettings:
    """
    Lazy proxy:
    - reads env var on first attribute access
    - creates a Settings instance and then delegates attribute access to it
    """
    def __init__(self, env_var: str = "LAZYDAG_SETTINGS_MODULE", default_module: str = "settings"):
        self._module: ModuleType | None = None
        self._env_var = os.getenv(env_var, default_module)

    def _setup(self) -> None:
        module_name = self._env_var
        try:
            self._module = import_module(module_name)
        except ImportError:
            raise ImportError(f"Could not import settings module: {module_name}")

    def __getattr__(self, name: str) -> Any:
        if self._module is None:
            self._setup()
        return getattr(self._module, name)

    def __setattr__(self, name: str, value: Any) -> None:
        # allow internal attrs
        if name.startswith("_"):
            return super().__setattr__(name, value)
        if self._module is None:
            self._setup()
        setattr(self._module, name, value)

    def configured(self) -> bool:
        return self._module is not None


settings = LazySettings()
