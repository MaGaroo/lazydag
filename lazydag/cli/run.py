import importlib
import os
import sys
import typer
from lazydag.pipeline import Pipeline
from lazydag.configs import RunConfig
from lazydag.scheduler import Scheduler


run_app = typer.Typer()


@run_app.command()
def run(py_module: str = "pipeline", project_config: str = "./config/run.yaml"):
    proj_cfg = RunConfig.from_file(project_config)
    proj_cfg.scaffold_data_dir()
    pipeline = Pipeline.from_yaml_file(proj_cfg.get_pipeline_path())
    sys.path.insert(0, os.getcwd())
    module = importlib.import_module(py_module)
    
    processes = module.processes
    collections = module.collections
    
    scheduler = Scheduler(pipeline, processes, collections)
    scheduler.start()


if __name__ == "__main__":
    run_app()
