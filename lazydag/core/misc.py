"""
it's hard to decide on the files for these codes
I'll write them here for now
"""
import importlib
from pathlib import Path
from lazydag.conf import settings


def scaffold_data_dir(data_root: Path):
    data_root.mkdir(exist_ok=True)
    (data_root / "configs").mkdir(exist_ok=True)


def get_processes_and_objects():
    module = importlib.import_module(settings.PY_MODULE)
    return module.processes, module.objects
