from typing import Dict, List, Optional, Set
from pathlib import Path
import yaml

class Pipeline:
    def __init__(self):
        self.processes = dict()
        self.objects = dict()

    @classmethod
    def from_yaml_file(cls, path: Path):
        if not path.exists():
            return None

        self = cls()
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        for obj in cfg["objects"]:
            self.add_object(obj)
        for proc_name, proc in cfg["processes"].items():
            self.add_process(proc_name, inputs=proc.get("inputs", {}), outputs=proc.get("outputs", {}))
        return self

    def to_yaml_file(self, path: Path):
        data = {
            "objects": sorted(self.objects.keys()),
            "processes": self.processes,
        }
        with open(path, "w") as f:
            yaml.safe_dump(data, f)

    def add_object(self, name: str):
        if name in self.objects:
            raise ValueError(f"Object {name} already exists")
        self.objects[name] = {
            "producer": None,
            "consumers": set()
        }

    def add_process(self, name: str, inputs: Dict[str, str], outputs: Dict[str, str]):
        if name in self.processes:
            raise ValueError(f"Process {name} already exists")

        for inp in inputs.values():
            if inp not in self.objects:
                raise ValueError(f"Input object {inp} does not exist")
            self.objects[inp]["consumers"].add(name)

        for out in outputs.values():
            if out not in self.objects:
                raise ValueError(f"Output object {out} does not exist")
            if self.objects[out]["producer"] is not None:
                raise ValueError(f"Output object {out} is already used by process {self.objects[out]["producer"]}")
            self.objects[out]["producer"] = name

        self.processes[name] = {"inputs": inputs, "outputs": outputs}

    def remove_process(self, name: str):
        if name not in self.processes:
            raise ValueError(f"Process {name} does not exist")

        for inp in self.processes[name]["inputs"].values():
            self.objects[inp]["consumers"].remove(name)
        for out in self.processes[name]["outputs"].values():
            self.objects[out]["producer"] = None
        del self.processes[name]

    def remove_object(self, name: str):
        if name not in self.objects:
            raise ValueError(f"Object {name} does not exist")

        if self.objects[name]["producer"] is not None:
            raise ValueError(f"Object {name} is still used by process {self.objects[name]["producer"]}")
        if len(self.objects[name]["consumers"]) > 0:
            raise ValueError(f"Object {name} is still used by processes {self.objects[name]["consumers"]}")

        del self.objects[name]

    def validate(self) -> List[str]:
        errors = []
        for obj_name, obj in self.objects.items():
            if obj["producer"] is None:
                errors.append(f"Object {obj_name} is not used by any process")
            if len(obj["consumers"]) == 0:
                errors.append(f"Object {obj_name} is not used by any process")

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
            for obj in self.processes[u]["outputs"].values():
                for v in self.objects[obj]["consumers"]:
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

    def object_consumers(self, object_name: str) -> Set[str]:
        return self.objects[object_name]["consumers"]

    def object_producer(self, object_name: str) -> Optional[str]:
        return self.objects[object_name]["producer"]
