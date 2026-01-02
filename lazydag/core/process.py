from abc import ABC, abstractmethod
from typing import List

class Process(ABC):
    """
    Base class for processes.
    """
    # These should be defined by subclasses
    inputs: List[str] = []
    outputs: List[str] = []
    has_daemon: bool = False

    def __init__(self, name: str):
        self.name = name

    def on_add_to_pipeline(self):
        """
        Optional method for processes.
        Triggered when process is added to pipeline.
        """
        pass

    def on_remove_from_pipeline(self):
        """
        Optional method for processes.
        Triggered when process is removed from pipeline.
        """
        pass

    def on_pipeline_start(self):
        """
        Optional method for processes.
        Triggered when pipeline execution starts.
        """
        pass

    def on_pipeline_end(self):
        """
        Optional method for processes.
        Triggered when pipeline execution ends.
        """
        pass

    def run_daemon(self, **kwargs):
        """
        Optional method for daemon processes.
        Run in a background thread. Has read access to outputs but should not modify them directly.
        """
        pass

    def poll(self, **kwargs):
        """
        Execution logic. kwargs will contain arguments matching inputs and outputs keys.
        Triggered when inputs change.
        """
        pass
