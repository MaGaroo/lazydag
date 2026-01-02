from pathlib import Path
import typer

FILE_CONTENTS = {
    "settings.py": """\
from pathlib import Path

PROJECT_NAME = "{project_name}"
DATA_ROOT = Path("./data")
PY_MODULE = "defs"

FS_OBJECTS = {{
    "save_dir": DATA_ROOT / "objects",
}}
""",
    "topology.yaml": """\
processes: {{}}
objects: []
""",
    "defs.py": """\
objects = []
processes = []
""",
}

def start_project(project_name: str):
    # TODO: change the template to use some features of lazydag
    project_path = Path(project_name)
    if project_path.exists():
        raise ValueError(f"Project {project_name} already exists")

    typer.echo(f"Creating project {project_name}")
    project_path.mkdir()
    for file, content in FILE_CONTENTS.items():
        typer.echo(f"Creating file {file}")
        with open(project_path / file, "w") as f:
            f.write(content.format(project_name=project_name))
