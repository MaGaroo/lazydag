from lazydag.core import Process
from lazydag.collections import ListObjectCollection
from lazydag.scheduler import Scheduler
import random

class GeneratorProcess(Process):
    inputs = []
    outputs = ["out"]
    def __init__(self, name):
        super().__init__(name)
        self.next_val = None

    def poll(self, out):
        if self.next_val is not None:
            out.push(self.next_val)

class ProxyProcess(Process):
    inputs = ["inp"]
    outputs = ["out"]
    def __init__(self, name):
        super().__init__(name)
        self.run_count = 0
        self.last_input_val = None

    def poll(self, inp, out):
        if not inp.changed():
            return
        self.run_count += 1
        if len(inp) > 0:
            last_val = inp.get(len(inp)-1)
            self.last_input_val = last_val
            # Propagate
            out.push(last_val)

def test_dependency_trigger(tmp_path):
    scheduler = Scheduler()
    scheduler.processes = {} # Reset
    scheduler._topology = []
    
    # Setup
    c1 = ListObjectCollection("c1", str(tmp_path/"c1"))
    c2 = ListObjectCollection("c2", str(tmp_path/"c2"))
    
    scheduler.register_collection(c1)
    scheduler.register_collection(c2)
    
    p0 = GeneratorProcess("p0")
    scheduler.register_process(p0, inputs={}, outputs={"out": c1})

    p1 = ProxyProcess("p1")
    scheduler.register_process(p1, inputs={"inp": c1}, outputs={"out": c2})
    
    # Trigger
    assert p1.run_count == 0
    p0.next_val = 42

    # Trigger scheduler step
    scheduler.step()
    
    assert p1.run_count == 1
    assert p1.last_input_val == 42
    assert c2.get(0) == 42
    
    p0.next_val = None
    assert not scheduler.step()
    assert p1.run_count == 1 # Unchanged

    p0.next_val = 43
    assert scheduler.step()

    assert p1.run_count == 2
    assert p1.last_input_val == 43
    assert c2.get(1) == 43


def test_chained_dependency(tmp_path):
    scheduler = Scheduler()
    scheduler.processes = {}
    scheduler._topology = []

    c1 = ListObjectCollection("c1", str(tmp_path/"c1"))
    c2 = ListObjectCollection("c2", str(tmp_path/"c2"))
    c3 = ListObjectCollection("c3", str(tmp_path/"c3"))
    
    scheduler.register_collection(c1)
    scheduler.register_collection(c2)
    scheduler.register_collection(c3)
    
    p0 = GeneratorProcess("p0")
    p1 = ProxyProcess("p1")
    p2 = ProxyProcess("p2")
    
    scheduler.register_process(p0, inputs={}, outputs={"out": c1})
    scheduler.register_process(p1, inputs={"inp": c1}, outputs={"out": c2})
    scheduler.register_process(p2, inputs={"inp": c2}, outputs={"out": c3})
    
    # Trigger chain
    p0.next_val = 100
    
    # Step 1: p1 sees change, runs. c2 updates. c1 and c2 saved.
    scheduler.step()
    
    assert p1.run_count == 1
    # Does p2 run in the same step?
    # Topological sort: p0 -> p1 -> p2.
    # p0 runs first. Updates c1.
    # c1 key "1" inserted. c1.changed() -> True.
    # p1 checked. Input c1 changed? True. p1 runs.
    # c2 key "1" inserted. c2.changed() -> True.
    # p2 checked. Input c2 changed? True. p2 runs.
    # Yes! In one step, changes propagate.
    
    assert p2.run_count == 1
    assert c3.get(0) == 100

