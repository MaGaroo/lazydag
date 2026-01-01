import typer

from lazydag.cli.pipeline import pipeline_app
from lazydag.cli.run import run_app


def main():
    app = typer.Typer()
    app.add_typer(pipeline_app, name="pipeline")
    app.add_typer(run_app, name="run")
    app()

if __name__ == "__main__":
    main()
