from concurrent.futures import FIRST_COMPLETED, wait, ThreadPoolExecutor
import threading
import time
from typing import Dict, List, Set, Iterable

from .object import Object
from .process import Process
from .pipeline import Pipeline

class Scheduler:
    def __init__(self, pipeline: Pipeline, processes: Iterable[Process], objects: Iterable[Object], parallelization: int = 4):
        self.pipeline: Pipeline = pipeline
        self.objects: Dict[str, Object] = {obj.name: obj for obj in objects}
        self.processes: Dict[str, Process] = {proc.name: proc for proc in processes}
        self.daemons: List[threading.Thread] = []
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=parallelization)
        self._assert_pipeline_consistent()

    def start(self):
        for obj in self.objects.values():
            obj.on_pipeline_start()
        for proc in self.processes.values():
            proc.on_pipeline_start()

        self.start_daemons()

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

        for obj in self.objects.values():
            obj.on_pipeline_end()
        for proc in self.processes.values():
            proc.on_pipeline_end()

        self.stop_daemons()

    def step(self):
        """
        Runs one iteration of the topological process loop.
        """
        process_pending_inputs = {name: len(self.pipeline.process_inputs(name)) for name in self.processes}
        ready_to_poll = lambda p: process_pending_inputs[p] == 0
        poll = lambda p: self.thread_pool.submit(self._poll_process, p)

        pending_processes = set()
        for proc_name, proc in self.processes.items():
            if ready_to_poll(proc_name):
                pending_processes.add(poll(proc_name))
        while pending_processes:
            done, pending_processes = wait(pending_processes, return_when=FIRST_COMPLETED)
            for future in done:
                proc_name = future.result()
                for out_obj in self.pipeline.process_outputs(proc_name).values():
                    for consumer in self.pipeline.object_consumers(out_obj):
                        process_pending_inputs[consumer] -= 1
                        if ready_to_poll(consumer):
                            pending_processes.add(poll(consumer))

        for obj in self.objects.values():
            obj.save()

    def start_daemons(self):
        for name, proc in self.processes.items():
            if proc.has_daemon:
                # Pass inputs/outputs as kwargs?
                # "It has read access to its outputs... but we guarantee it will not change it."
                # We pass the same objects.
                kwargs = self._get_process_args(name)

                # Daemon thread
                t = threading.Thread(target=proc.run_daemon, kwargs=kwargs, name=f"daemon-{name}", daemon=True)
                t.start()
                self.daemons.append(t)
        return

    def stop_daemons(self):
        for t in self.daemons:
            t.join()
        return

    def _assert_pipeline_consistent(self):
        """
        Assert that the pipeline is consistent with the processes and objects.
        """
        assert set(self.processes.keys()) == set(self.pipeline.processes.keys()), \
            str(self.processes.keys()) + " != " + str(self.pipeline.processes.keys())
        for proc_name, proc in self.processes.items():
            assert proc_name == proc.name
            assert set(proc.inputs) == set(self.pipeline.process_inputs(proc_name).keys())
            assert set(proc.outputs) == set(self.pipeline.process_outputs(proc_name).keys())

        assert len(self.objects) == len(self.pipeline.objects)
        for obj_name, obj in self.objects.items():
            assert obj_name == obj.name
            assert obj_name in self.pipeline.objects

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
        for input_port, obj_name in self.pipeline.process_inputs(proc_name).items():
            kwargs[input_port] = self.objects[obj_name]
        for output_port, obj_name in self.pipeline.process_outputs(proc_name).items():
            kwargs[output_port] = self.objects[obj_name]
        return kwargs
