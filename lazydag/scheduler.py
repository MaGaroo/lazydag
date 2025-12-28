import threading
import time
from typing import Dict, List, Set, Any
from collections import defaultdict
from .core import Process, ObjectCollection

class Scheduler:
    def __init__(self):
        self.collections: Dict[str, ObjectCollection] = {}
        self.processes: Dict[str, Process] = {}
        self.daemons: List[threading.Thread] = []
        self.process_inputs: Dict[str, Dict[str, List[ObjectCollection]]] = {}
        self.process_outputs: Dict[str, Dict[str, List[ObjectCollection]]] = {}
        self._topology: List[str] = [] # List of process names in topological order

    def register_collection(self, collection: ObjectCollection):
        self.collections[collection.name] = collection

    def register_process(self, process: Process, inputs: Dict[str, ObjectCollection], outputs: Dict[str, ObjectCollection]):
        if process.name in self.processes:
            raise ValueError(f"Process {process.name} already registered")
        self.processes[process.name] = process
        self.process_inputs[process.name] = inputs or {}
        self.process_outputs[process.name] = outputs or {}

    def _compute_topology(self):
        """
        Computes the topological order of processes based on data dependencies.
        A depends on B if B produces a collection that A inputs.
        """
        # 1. Map collections to their producers
        producer: Dict[str, str] = dict() # collection_name -> process_name
        
        for name, proc in self.processes.items():
            for out_col in self.process_outputs[name].values():
                if out_col in producer:
                    raise ValueError(f"Collection {out_col} is produced by multiple processes: {producer[out_col]} and {name}")
                producer[out_col] = name
        
        # 2. Build dependency graph (Adjacency list: consumer -> [producers])
        proc_dependencies: Dict[str, Set[str]] = {name: set() for name in self.processes}
        
        for name, proc in self.processes.items():
            for in_col in self.process_inputs[name].values():
                if in_col not in producer:
                    raise ValueError(f"Collection {in_col} is not produced by any process")
                producer_name = producer[in_col]
                # Self-dependency shouldn't happen in DAG, but if it does, ignore or handle?
                if producer_name != name:
                    proc_dependencies[name].add(producer_name)
        
        # 3. Topological Sort (Kahn's algorithm)
        proc_dependants = defaultdict(list) # producer -> [consumers]
        for consumer, producers_in_deps in proc_dependencies.items():
            for producer in producers_in_deps:
                proc_dependants[producer].append(consumer)
                
        in_degree = {name: len(deps) for name, deps in proc_dependencies.items()}
        queue = [name for name, deg in in_degree.items() if deg == 0]
        topo_order = []

        while queue:
            u = queue.pop(0)
            topo_order.append(u)
            
            for v in proc_dependants[u]:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)
        
        if len(topo_order) != len(self.processes):
            raise ValueError("Cycle detected in process dependencies")
            
        self._topology = topo_order
        
    def step(self):
        """
        Runs one iteration of the topological process loop.
        """
        if not self._topology:
            self._compute_topology()

        # Iterate over topological order
        for proc_name in self._topology:
            proc = self.processes[proc_name]
            
            # Check if triggered
            # "if one of its inputs had changed, we will trigger it. Otherwise, not."
            # TODO: find a better way to handle daemons
            inputs_changed = any(inp_col.changed() for inp_col in self.process_inputs[proc_name].values())
            no_inputs = len(self.process_inputs[proc_name]) == 0
            
            if inputs_changed or no_inputs:
                 # Prepare args
                kwargs = self._get_process_args(proc_name)
                
                proc.poll(**kwargs)
        
        # 4. Save changed collections
        for col in self.collections.values():
            if col.changed():
                col.save()

    def start(self):
        # 1. Compute topology
        self._compute_topology()
        
        # 2. Start daemons
        for name, proc in self.processes.items():
            if proc.has_daemon:
                # Pass inputs/outputs as kwargs?
                # "It has read access to its outputs... but we guarantee it will not change it."
                # We pass the same collections.
                kwargs = self._get_process_args(name)
                
                # Daemon thread
                t = threading.Thread(target=proc.run_daemon, kwargs=kwargs, name=f"daemon-{name}", daemon=True)
                t.start()
                self.daemons.append(t)
        
        # 3. Main Loop
        try:
            while True:
                self.step()
                
                # Sleep to avoid CPU spin? The user didn't specify.
                # If we have run_daemons producing data, we want to pick it up fast.
                # But minimal sleep is good practice.
                time.sleep(0.01)
                
        except KeyboardInterrupt:
            pass

    def _get_process_args(self, proc_name: str):
        kwargs = {}
        for inp, col in self.process_inputs[proc_name].items():
            kwargs[inp] = col
        for out, col in self.process_outputs[proc_name].items():
            kwargs[out] = col
        return kwargs

# Global singleton
_runtime = Scheduler()

def get_runtime():
    return _runtime
