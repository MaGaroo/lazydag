from pathlib import Path
import yaml

class RunConfig:
    def __init__(self, project_name: str, source_root: str, data_root: str):
        self.project_name = Path(project_name)
        self.source_root = Path(source_root)
        self.data_root = Path(data_root)

    @classmethod
    def from_file(cls, path: Path):
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
            return cls(**cfg)

    def get_pipeline_path(self) -> Path:
        return self.data_root / "configs" / "pipeline.yaml"

    def scaffold_data_dir(self):
        if self.data_root.exists():
            return
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.get_pipeline_path().parent.mkdir(parents=True, exist_ok=True)
        self.get_pipeline_path().write_text("collections: []\nprocesses: []")
