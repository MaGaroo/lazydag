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

    @abstractmethod
    def poll(self, **kwargs):
        """
        Execution logic. kwargs will contain arguments matching inputs and outputs keys.
        Triggered when inputs change.
        """
        pass

    def run_daemon(self, **kwargs):
        """
        Optional method for daemon processes.
        Run in a background thread. Has read access to outputs but should not modify them directly.
        """
        pass
