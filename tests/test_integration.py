import pytest
import time
import pickle
from lazydag.core import Process
from lazydag.schematic import define_collection, define_process

from lazydag.collections import ListObjectCollection
from lazydag.scheduler import Scheduler, get_runtime

# Clean singleton for test? 
# The global singleton `_runtime` persists. Ideally tests should be isolated.
# However, we can just instantiate a new runtime or clear it?
# The `get_runtime` returns the global one.
# For tests, calling `define_*` uses the global variable.
# We should probably reset the global runtime.

@pytest.fixture(autouse=True)
def reset_scheduler():
    from lazydag import scheduler
    scheduler._runtime = scheduler.Scheduler()
    # Also stop old daemons? 
    # Since we use non-daemon threads now, previous tests might leave threads running.
    # But previous tests (test_scheduler) didn't use `define_daemon`, just Thread manually (Wait, test_scheduler didn't test daemon).
    # So we should be fine.
    yield
    # Cleanup threads if any?
    scheduler._runtime.daemons = []

class DeterministicSource(Process):
    inputs = []
    outputs = ["out"]
    def __init__(self, name):
        super().__init__(name)
        self.counter = 0

    def poll(self, out):
        if self.counter < 5:
            out.push(self.counter)
            self.counter += 1

class FilterEvenProcess(Process):
    inputs = ["inp"]
    outputs = ["out"]
    def __init__(self, name):
        super().__init__(name)
        self.idx = 0
    def poll(self, inp, out):
        while self.idx < len(inp):
            val = inp.get(self.idx)
            if val % 2 == 0:
                out.push(val)
            self.idx += 1

def test_full_pipeline(tmp_path):
    # collections
    c1 = define_collection(ListObjectCollection, "c1", str(tmp_path/"c1"))
    c2 = define_collection(ListObjectCollection, "c2", str(tmp_path/"c2"))
    
    # processes
    define_process(DeterministicSource, "source", inputs={}, outputs={"out": c1})
    define_process(FilterEvenProcess, "filter", inputs={"inp": c1}, outputs={"out": c2})
    
    scheduler = get_runtime()
    
    # Run steps
    # We need enough steps. 
    # Step 1: Source inserts 0.
    # Step 2: Source inserts 1. Filter sees 0 -> inserts 0.
    # Step 3: Source inserts 2. Filter sees 1 -> skips.
    # ...
    # Let's run 10 steps to be safe.
    for _ in range(10):
        scheduler.step()
    
    # Check memory state
    # c1: [0, 1, 2, 3, 4]
    # c2: [0, 2, 4]
    assert len(c1) == 5
    assert len(c2) == 3
    assert c2.get(0) == 0
    assert c2.get(1) == 2
    assert c2.get(2) == 4
    
    # Check persistence
    # Collections should be already saved by scheduler.step() logic if changed.
    # Just verify files exist and content.
    
    assert (tmp_path/"c1").exists()
    assert (tmp_path/"c2").exists()
    
    with open(tmp_path/"c2", "rb") as f:
        data = pickle.load(f)
        assert data == [0, 2, 4]
