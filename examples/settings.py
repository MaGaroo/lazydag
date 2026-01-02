from pathlib import Path

PROJECT_NAME = "lazydag-example"
DATA_ROOT = Path("./data")
PY_MODULE = "pipeline"

FS_OBJECTS = {
    "save_dir": DATA_ROOT / "objects",
}
