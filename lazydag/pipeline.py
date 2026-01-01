from typing import Dict, List, Optional, Set
from pathlib import Path
import yaml

class Pipeline:
    def __init__(self):
        self.processes = dict()
        self.collections = dict()

    @classmethod
    def from_yaml_file(cls, path: Path):
        self = cls()
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        for coll in cfg["collections"]:
            self.add_collection(coll)
        for proc_name, proc in cfg["processes"].items():
            self.add_process(proc_name, inputs=proc["inputs"], outputs=proc["outputs"])
        return self

    def to_yaml_file(self, path: Path):
        data = {
            "collections": sorted(self.collections.keys()),
            "processes": self.processes,
        }
        with open(path, "w") as f:
            yaml.safe_dump(data, f)

    def add_collection(self, name: str):
        if name in self.collections:
            raise ValueError(f"Collection {name} already exists")
        self.collections[name] = {
            "producer": None,
            "consumers": set()
        }

    def add_process(self, name: str, inputs: Dict[str, str], outputs: Dict[str, str]):
        if name in self.processes:
            raise ValueError(f"Process {name} already exists")

        for inp in inputs.values():
            if inp not in self.collections:
                raise ValueError(f"Input collection {inp} does not exist")
            self.collections[inp]["consumers"].add(name)

        for out in outputs.values():
            if out not in self.collections:
                raise ValueError(f"Output collection {out} does not exist")
            if self.collections[out]["producer"] is not None:
                raise ValueError(f"Output collection {out} is already used by process {self.collections[out]["producer"]}")
            self.collections[out]["producer"] = name

        self.processes[name] = {"inputs": inputs, "outputs": outputs}

    def remove_process(self, name: str):
        if name not in self.processes:
            raise ValueError(f"Process {name} does not exist")

        for inp in self.processes[name]["inputs"].values():
            self.collections[inp]["consumers"].remove(name)
        for out in self.processes[name]["outputs"].values():
            self.collections[out]["producer"] = None
        del self.processes[name]

    def remove_collection(self, name: str):
        if name not in self.collections:
            raise ValueError(f"Collection {name} does not exist")

        if self.collections[name]["producer"] is not None:
            raise ValueError(f"Collection {name} is still used by process {self.collections[name]["producer"]}")
        if len(self.collections[name]["consumers"]) > 0:
            raise ValueError(f"Collection {name} is still used by processes {self.collections[name]["consumers"]}")

        del self.collections[name]

    def validate(self) -> List[str]:
        errors = []
        for coll_name, coll in self.collections.items():
            if coll["producer"] is None:
                errors.append(f"Collection {coll_name} is not used by any process")
            if len(coll["consumers"]) == 0:
                errors.append(f"Collection {coll_name} is not used by any process")

        try:
            self.topological_sort()
        except ValueError as e:
            errors.append(str(e))

        return errors

    def topological_sort(self) -> List[str]:
        input_degree = {proc: len(self.processes[proc]["inputs"]) for proc in self.processes}
        queue = {proc for proc, degree in input_degree.items() if degree == 0}
        topological_order = []
        while len(queue) > 0:
            u = queue.pop()
            topological_order.append(u)
            for coll in self.processes[u]["outputs"].values():
                for v in self.collections[coll]["consumers"]:
                    input_degree[v] -= 1
                    if input_degree[v] == 0:
                        queue.add(v)
        if len(topological_order) != len(self.processes):
            raise ValueError("Graph contains a cycle")
        return topological_order

    def process_inputs(self, process_name: str) -> Dict[str, str]:
        return self.processes[process_name]["inputs"]

    def process_outputs(self, process_name: str) -> Dict[str, str]:
        return self.processes[process_name]["outputs"]

    def collection_consumers(self, collection_name: str) -> Set[str]:
        return self.collections[collection_name]["consumers"]

    def collection_producer(self, collection_name: str) -> Optional[str]:
        return self.collections[collection_name]["producer"]
