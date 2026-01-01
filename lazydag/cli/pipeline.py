from typing import List
from pathlib import Path
from typing import Dict
import typer
from lazydag.pipeline import Pipeline
from lazydag.configs import RunConfig

pipeline_app = typer.Typer()


@pipeline_app.command()
def add_process(process_name: str, project_config: str = "./config/run.yaml", inputs: List[str] = [], outputs: List[str] = []):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    inputs = {inp.split(":")[0]: inp.split(":")[1] for inp in inputs}
    outputs = {out.split(":")[0]: out.split(":")[1] for out in outputs}
    pipeline.add_process(process_name, inputs, outputs)
    pipeline.to_yaml_file(proj_cfg.get_pipeline_path())


@pipeline_app.command()
def remove_process(process_name: str, project_config: str = "./config/run.yaml"):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    pipeline.remove_process(process_name)
    pipeline.to_yaml_file(proj_cfg.get_pipeline_path())


@pipeline_app.command()
def add_collection(collection_name: str, project_config: str = "./config/run.yaml"):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    pipeline.add_collection(collection_name)
    pipeline.to_yaml_file(proj_cfg.get_pipeline_path())


@pipeline_app.command()
def remove_collection(collection_name: str, project_config: str = "./config/run.yaml"):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    pipeline.remove_collection(collection_name)
    pipeline.to_yaml_file(proj_cfg.get_pipeline_path())


@pipeline_app.command()
def validate(project_config: str = "./config/run.yaml"):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    errors = pipeline.validate()
    if len(errors) > 0:
        for err in errors:
            print(err)
        raise ValueError("Pipeline is invalid")
    print("Pipeline is valid")


if __name__ == "__main__":
    pipeline_app()
