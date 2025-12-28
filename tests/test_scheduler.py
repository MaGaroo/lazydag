from lazydag.core import Process
from lazydag.collections import ListObjectCollection
from lazydag.scheduler import Scheduler

class MockProcess(Process):
    inputs = ["inp"]
    outputs = ["out"]
    def __init__(self, name):
        super().__init__(name)
        self.run_count = 0
        self.last_input_val = None

    def poll(self, inp, out):
        self.run_count += 1
        if len(inp) > 0:
            self.last_input_val = inp.get(len(inp)-1)
            # Propagate
            out.push(self.last_input_val)

def test_dependency_trigger(tmp_path):
    scheduler = Scheduler()
    scheduler.processes = {} # Reset
    scheduler._topology = []
    
    # Setup
    c1 = ListObjectCollection("c1", str(tmp_path/"c1"))
    c2 = ListObjectCollection("c2", str(tmp_path/"c2"))
    
    scheduler.register_collection(c1)
    scheduler.register_collection(c2)
    
    proc = MockProcess("p1")
    scheduler.register_process(proc, inputs={"inp": c1}, outputs={"out": c2})
    
    # Trigger
    assert proc.run_count == 0
    c1.insert(0, 42)
    
    # Trigger scheduler step
    scheduler.step()
    
    assert proc.run_count == 1
    assert proc.last_input_val == 42
    assert c2.get(0) == 42
    
    # c2.changed() will be False because step() saves changes
    # assert c2.changed() <- REMOVED

    
    # Another step, c1 unchanged.
    # Current behavior: poll runs only if inputs changed.
    # c1 hasn't changed since last step?
    # Wait, c1.insert added to changelog.
    # step() ran poll. poll finished.
    # step() saves changed collections.
    # c1.save() called implicitly? Yes, Step 4 of scheduler step.
    # So c1.changelog is cleared. c1.changed() -> False.
    # Next step: changed() is False. poll() not called.
    
    scheduler.step()
    assert proc.run_count == 1 # Unchanged

    c1.insert(1, 43)
    scheduler.step()
    
    assert proc.run_count == 2
    assert proc.last_input_val == 43
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
    
    p1 = MockProcess("p1")
    p2 = MockProcess("p2")
    
    scheduler.register_process(p1, inputs={"inp": c1}, outputs={"out": c2})
    scheduler.register_process(p2, inputs={"inp": c2}, outputs={"out": c3})
    
    # Trigger chain
    c1.insert(0, 100)
    
    # Step 1: p1 sees change, runs. c2 updates. c1 and c2 saved.
    scheduler.step()
    
    assert p1.run_count == 1
    # Does p2 run in the same step?
    # Topological sort: p1 -> p2.
    # p1 runs first. Updates c2.
    # c2 key "1" inserted. c2.changed() -> True.
    # p2 checked. Input c2 changed? True. p2 runs.
    # Yes! In one step, changes propagate.
    
    assert p2.run_count == 1
    assert c3.get(0) == 100

