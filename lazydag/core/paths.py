from lazydag.conf import settings

def get_pipeline_path():
    return settings.DATA_ROOT / "configs" / "pipeline.yaml"
