import os
import sys
import typer

from lazydag.cli.topology import topology
from lazydag.cli.run import run_app
from lazydag.conf import settings
from lazydag.core.misc import scaffold_data_dir
from lazydag.core.pipeline import Pipeline
from lazydag.core.paths import get_pipeline_path


def main():
    app = typer.Typer()
    app.add_typer(topology, name="topology", callback=callback)
    app.add_typer(run_app, name="run", callback=callback)
    app()


def callback(ctx: typer.Context):
    sys.path.insert(0, os.getcwd())

    scaffold_data_dir(settings.DATA_ROOT)
    pipeline = Pipeline.from_yaml_file(get_pipeline_path())

    ctx.ensure_object(dict)
    ctx.obj["pipeline"] = pipeline


if __name__ == "__main__":
    main()
