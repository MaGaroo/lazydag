from typing import Type, Dict, Any, Optional
from .core import Process, ObjectCollection
from .scheduler import get_runtime

def define_collection(cls: Type[ObjectCollection], name: str, save_path: str) -> ObjectCollection:
    """
    Defines and registers a new object collection.
    """
    collection = cls(name, save_path)
    get_runtime().register_collection(collection)
    return collection

def define_process(cls: Type[Process], name: str, inputs: Dict[str, ObjectCollection] = None, outputs: Dict[str, ObjectCollection] = None) -> Process:
    """
    Defines and registers a new process.
    """
    process = cls(name)
    get_runtime().register_process(process, inputs, outputs)
    return process
