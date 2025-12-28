from typing import Any, List, Dict, Optional, Union
import copy
import pickle
import os
from .core import ObjectCollection

class ListObjectCollection(ObjectCollection):
    def __init__(self, name: str, save_path: str):
        super().__init__(name, save_path)
        # _current is the in-memory view of the up-to-date data
        self._current: List[Any] = copy.deepcopy(self._data)

    def _get_empty_structure(self) -> Any:
        return []

    def get(self, idx: int, old: bool = False) -> Any:
        if old:
            return self._data[idx]
        else:
            return self._current[idx]
    
    def insert(self, idx: int, value: Any):
        self._current.insert(idx, value)
        self._changelog.append(('insert', idx, value))

    def push(self, value: Any):
        """Convenience method to append to the end of the list."""
        self.insert(len(self), value)
    
    def remove(self, idx: int):
        # We need to store enough info to replay or just rely on _current being the source of truth for saving?
        # The prompt says "collection must hold a log of changes".
        # If we use _current to overwrite _data on save, the log is technically redundant for calculation but required by spec "hold a log".
        # We will store the op.
        assert idx < len(self)
        val = self._current.pop(idx)
        self._changelog.append(('remove', idx, val)) # Storing val just in case debug needed

    def set(self, idx: int, value: Any):
        if self._current[idx] == value:
            return
        self._current[idx] = value
        self._changelog.append(('set', idx, value))

    def save(self):
        # Update persistent data
        self._data = copy.deepcopy(self._current)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.save_path), exist_ok=True)
        
        with open(self.save_path, 'wb') as f:
            pickle.dump(self._data, f)
        
        # Clear change history
        self._changelog.clear()

    def __len__(self):
        return len(self._current)

    def __iter__(self):
        return iter(self._current)

    def __getitem__(self, idx: int):
        return self._current[idx]


class DictObjectCollection(ObjectCollection):
    def __init__(self, name: str, save_path: str):
        super().__init__(name, save_path)
        self._current: Dict[Any, Any] = copy.deepcopy(self._data)

    def _get_empty_structure(self) -> Any:
        return {}

    def get(self, key: Any, old: bool = False) -> Any:
        if old:
            return self._data.get(key)
        else:
            return self._current.get(key)

    def set(self, key: Any, value: Any):
        self._current[key] = value
        self._changelog.append(('set', key, value))

    def remove(self, key: Any):
        if key in self._current:
            del self._current[key]
            self._changelog.append(('remove', key))

    def save(self):
        self._data = copy.deepcopy(self._current)
        
        os.makedirs(os.path.dirname(self.save_path), exist_ok=True)
        
        with open(self.save_path, 'wb') as f:
            pickle.dump(self._data, f)
        
        self._changelog.clear()
        
    def __len__(self):
        return len(self._current)

    def __iter__(self):
        return iter(self._current)

    def __getitem__(self, key: Any):
        return self._current[key]
