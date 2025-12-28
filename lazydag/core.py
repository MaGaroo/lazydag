from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import copy
import os
import pickle

class ObjectCollection(ABC):
    """
    Base class for all object collections.
    Manages filesystem path and abstract save/load mechanism.
    """
    def __init__(self, name: str, save_path: str):
        self.name = name
        self.save_path = save_path
        # _data represents the persistent state (on disk)
        self._data: Any = None
        # _changelog represents the changes in memory since last save
        self._changelog: List[Any] = []
        
        # Initialize data (could be lazy loaded, but for now let's say we check existence)
        self._load()

    def _load(self):
        """Loads data from filesystem if exists, else initializes empty."""
        if os.path.exists(self.save_path):
            with open(self.save_path, 'rb') as f:
                self._data = pickle.load(f)
        else:
            self._data = self._get_empty_structure()

    @abstractmethod
    def _get_empty_structure(self) -> Any:
        pass

    @abstractmethod
    def save(self):
        """
        Updates the data in the filesystem and removes change history.
        """
        pass
    
    def changed(self) -> bool:
        """Returns True if the changelog is not empty."""
        return len(self._changelog) > 0
    
    @abstractmethod
    def get(self, old: bool = False, **kwargs) -> Any:
        """
        Returns a view of the data.
        If old=True, returns the filesystem version (without changelog).
        If old=False, returns the current version (with changelog applied).
        """
        pass


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

