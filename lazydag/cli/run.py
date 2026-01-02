import typer
from lazydag.core.misc import get_processes_and_objects
from lazydag.core.pipeline import Pipeline
from lazydag.core.scheduler import Scheduler

run_app = typer.Typer()


@run_app.command()
def run(ctx: typer.Context):
    pipeline: Pipeline = ctx.obj["pipeline"]
    if pipeline is None:
        typer.echo("Error: pipeline not found, have you built it?")
        return
    processes, objects = get_processes_and_objects()
    scheduler = Scheduler(pipeline, processes, objects)
    scheduler.start()


if __name__ == "__main__":
    run_app()
