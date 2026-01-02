from pathlib import Path
from typing import List, Dict
import typer
from lazydag.core.object import Object
from lazydag.core.pipeline import Pipeline
from lazydag.core.paths import get_pipeline_path
from lazydag.cli.utils import get_object_by_name


topology_app = typer.Typer()


@topology_app.command()
def add_process(ctx: typer.Context, process_name: str, inputs: List[str] = [], outputs: List[str] = []):
    pipeline: Pipeline = ctx.obj["pipeline"]
    inputs: Dict[str, str] = {inp.split(":")[0]: inp.split(":")[1] for inp in inputs}
    outputs: Dict[str, str] = {out.split(":")[0]: out.split(":")[1] for out in outputs}
    pipeline.add_process(process_name, inputs, outputs)
    pipeline.to_yaml_file(get_pipeline_path())


@topology_app.command()
def remove_process(ctx: typer.Context, process_name: str):
    pipeline: Pipeline = ctx.obj["pipeline"]
    pipeline.remove_process(process_name)
    pipeline.to_yaml_file(get_pipeline_path())


@topology_app.command()
def add_object(ctx: typer.Context, object_name: str):
    pipeline: Pipeline = ctx.obj["pipeline"]
    pipeline.add_object(object_name)
    pipeline.to_yaml_file(get_pipeline_path())

    object: Object = get_object_by_name(object_name)
    object.on_add_to_pipeline()


@topology_app.command()
def remove_object(ctx: typer.Context, object_name: str):
    pipeline: Pipeline = ctx.obj["pipeline"]
    pipeline.remove_object(object_name)
    pipeline.to_yaml_file(get_pipeline_path())

    object: Object = get_object_by_name(object_name)
    object.on_remove_from_pipeline()


@topology_app.command()
def from_yaml(ctx: typer.Context, yaml_addr: Path):
    if not yaml_addr.exists():
        typer.echo(f"Error: topology file {yaml_addr} does not exist")
        return

    pipeline: Pipeline = ctx.obj["pipeline"]
    if pipeline is not None:
        typer.echo("Error: pipeline already exists, remove it first")
        return

    pipeline = Pipeline.from_yaml_file(yaml_addr)
    assert pipeline is not None

    pipeline.to_yaml_file(get_pipeline_path())
    for obj_name in pipeline.objects:
        obj = get_object_by_name(obj_name)
        obj.on_add_to_pipeline()


@topology_app.command()
def validate(ctx: typer.Context):
    pipeline: Pipeline = ctx.obj["pipeline"]
    errors = pipeline.validate()
    if len(errors) > 0:
        for err in errors:
            print(err)
        raise ValueError("Pipeline is invalid")
    print("Pipeline is valid")


if __name__ == "__main__":
    topology_app()
