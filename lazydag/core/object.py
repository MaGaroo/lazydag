import shutil
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import copy
import os
import pickle


class Object(ABC):
    """
    Base class for all objects.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def on_add_to_pipeline(self):
        """
        This function is called once the object is added to the pipeline.
        """
        pass

    @abstractmethod
    def on_remove_from_pipeline(self):
        """
        This function is called once the object is removed from the pipeline.
        """
        pass

    @abstractmethod
    def on_pipeline_start(self):
        """
        This function is called once the pipeline execution starts.
        """
        pass

    @abstractmethod
    def on_pipeline_end(self):
        """
        This function is called once the pipeline execution ends.
        """
        pass

    @abstractmethod
    def save(self):
        """
        This function is called once one iteration of the pipeline is completed.
        Therefore, if there are unsaved changes, they have to be saved permanently.
        """
        pass

    @abstractmethod
    def purge(self):
        """
        This function is called when the contents of the object need to be reset
        to the initial state, i.e. the same state as after calling on_add_to_pipeline.

        An example use case is when the producer of the object is changed and needs to
        be re-executed.
        """
        pass

