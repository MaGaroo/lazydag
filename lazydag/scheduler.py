from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, wait, ThreadPoolExecutor
import threading
import time
from typing import Dict, List, Set
from .core import Process, ObjectCollection

class Scheduler:
    def __init__(self, parallelization: int = 4):
        self.collections: Dict[str, ObjectCollection] = {}
        self.processes: Dict[str, Process] = {}
        self.daemons: List[threading.Thread] = []
        self.process_inputs: Dict[str, Dict[str, List[ObjectCollection]]] = {}
        self.process_outputs: Dict[str, Dict[str, List[ObjectCollection]]] = {}
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=parallelization)
        self.collection_consumers: Dict[str, List[str]] = {}

    def register_collection(self, collection: ObjectCollection):
        if collection.name in self.collections:
            raise ValueError(f"Collection {collection.name} already registered")
        self.collections[collection.name] = collection
        self.collection_consumers[collection.name] = []

    def register_process(self, process: Process, inputs: Dict[str, ObjectCollection], outputs: Dict[str, ObjectCollection]):
        if process.name in self.processes:
            raise ValueError(f"Process {process.name} already registered")
        self.processes[process.name] = process
        self.process_inputs[process.name] = inputs or {}
        self.process_outputs[process.name] = outputs or {}
        for col in inputs.values():
            if col.name not in self.collections:
                raise ValueError(f"Collection {col.name} not registered")
            self.collection_consumers[col.name].append(process.name)

    def step(self) -> bool:
        """
        Runs one iteration of the topological process loop.
        Returns True if any collection was changed.
        """
        process_pending_inputs = {name: len(self.process_inputs[name]) for name in self.processes}
        can_poll = lambda p: process_pending_inputs[p] == 0
        poll = lambda p: self.thread_pool.submit(self._poll_process, p)

        pending_processes = set()
        for proc_name, proc in self.processes.items():
            if can_poll(proc_name):
                pending_processes.add(poll(proc_name))
        while pending_processes:
            done, pending_processes = wait(pending_processes, return_when=FIRST_COMPLETED)
            for future in done:
                proc_name = future.result()
                for out_col in self.process_outputs[proc_name].values():
                    for consumer in self.collection_consumers[out_col.name]:
                        process_pending_inputs[consumer] -= 1
                        if can_poll(consumer):
                            pending_processes.add(poll(consumer))
        
        changed = False
        for col in self.collections.values():
            if col.changed():
                col.save()
                changed = True

        return changed

    def start(self):
        # Start daemons
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
        
        # Main Loop
        try:
            while True:
                if self.step():
                    print("-=-=-=-=-=-=-=-=-=-")
                
                # Sleep to avoid CPU spin? The user didn't specify.
                # If we have run_daemons producing data, we want to pick it up fast.
                # But minimal sleep is good practice.
                time.sleep(0.01)
                
        except KeyboardInterrupt:
            pass

    def _poll_process(self, proc_name: str):
        """
        Poll a process and return the name of the process.
        """
        proc = self.processes[proc_name]
        args = self._get_process_args(proc_name)
        proc.poll(**args)

        # Return the name of the process to track process in threadpool futures
        return proc_name

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
