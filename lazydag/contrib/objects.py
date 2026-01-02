import copy
import os
from pathlib import Path
import pickle
import shutil
from typing import Any, List, Tuple

from lazydag.core.object import Object
from lazydag.conf import settings

class FSBackedObject(Object):
    """
    Base class for all objects stored on the filesystem.
    Manages filesystem path and abstract save/load mechanism.
    """
    def __init__(self, name: str, save_path: Path = None):
        super().__init__(name)
        self.save_path: Path = save_path or Path(settings.FS_OBJECTS["save_dir"]) / name

    def on_add_to_pipeline(self):
        self.save_path.mkdir(parents=True)

    def on_remove_from_pipeline(self):
        shutil.rmtree(self.save_path)

    def purge(self):
        for child in os.listdir(self.save_path):
            shutil.rmtree(self.save_path / child)

    def __str__(self):
        return f"{self.__class__.__name__}<{self.name}>"


class FSListObject(FSBackedObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._changelog: List[Tuple[str, int, Any]] = []

    def _get_data_path(self) -> Path:
        return self.save_path / "data.pkl"

    def _get_empty_structure(self) -> Any:
        return []

    def on_pipeline_start(self):
        data_path = self._get_data_path()
        if data_path.exists():
            with data_path.open("rb") as f:
                self._data = pickle.load(f)
        else:
            self._data = self._get_empty_structure()
        self._current = copy.deepcopy(self._data)

    def on_pipeline_end(self):
        pass

    def get(self, idx: int, old: bool = False) -> Any:
        if old:
            return self._data[idx]
        else:
            return self._current[idx]

    def insert(self, idx: int, value: Any):
        if idx < 0 or idx > len(self):
            raise ValueError("Index out of bounds")
        self._current.insert(idx, value)
        self._changelog.append(('insert', idx, value))

    def push(self, value: Any):
        """Convenience method to append to the end of the list."""
        self.insert(len(self), value)
    
    def remove(self, idx: int):
        if idx < 0 or idx >= len(self):
            raise ValueError("Index out of bounds")
        val = self._current.pop(idx)
        self._changelog.append(('remove', idx, val))

    def set(self, idx: int, value: Any):
        if idx < 0 or idx >= len(self):
            raise ValueError("Index out of bounds")
        if self._current[idx] == value:
            return
        self._current[idx] = value
        self._changelog.append(('set', idx, value))

    def save(self):
        # Update persistent data
        self._data = copy.deepcopy(self._current)

        data_path = self._get_data_path()
        with data_path.open("wb") as f:
            pickle.dump(self._data, f)

        # Clear change history
        self._changelog.clear()

    def changed(self) -> bool:
        return len(self._changelog) > 0

    def __len__(self):
        return len(self._current)

    def __iter__(self):
        return iter(self._current)

    def __getitem__(self, idx: int):
        return self._current[idx]


class FSDictObject(FSBackedObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_data_path(self) -> Path:
        return self.save_path / "data.pkl"

    def _get_empty_structure(self) -> Any:
        return {}

    def on_pipeline_start(self):
        data_path = self._get_data_path()
        if data_path.exists():
            with data_path.open("rb") as f:
                self._data = pickle.load(f)
        else:
            self._data = self._get_empty_structure()
        self._current = copy.deepcopy(self._data)

    def on_pipeline_end(self):
        pass

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

    def changed(self) -> bool:
        return len(self._changelog) > 0

    def __len__(self):
        return len(self._current)

    def __iter__(self):
        return iter(self._current)

    def __contains__(self, key: Any):
        return key in self._current

    def keys(self):
        return self._current.keys()

    def values(self):
        return self._current.values()

    def items(self):
        return self._current.items()

    def __getitem__(self, key: Any):
        return self._current[key]
