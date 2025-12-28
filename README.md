# LazyDAG

LazyDAG is a python framework that manages a pipeline for data processing.
The focus is on incremental data processing and reactive execution.
That means, if an input asset consists of multiple entries and one of them
changes, the process can only recompute the changed entry.

## Features
- **ObjectCollection**: Managed data storage with incremental updates and filesystem persistence.
- **Process**: Units of computation that react to changes in input collections.
- **Lazy/Reactive Execution**: Processes run only when their inputs change.
- **Daemons**: Background processes that continuously feed data into the pipeline.

## Usage

```python
from lazydag.schematic import define_process, define_collection
from lazydag.collections import ListObjectCollection
from lazydag.core import Process
from lazydag.scheduler import get_runtime

# Define your collections
oc1 = define_collection(ListObjectCollection, name="Numbers", save_path="./data/nums")
oc2 = define_collection(ListObjectCollection, name="Results", save_path="./data/results")

# Define your processes
class MyProcess(Process):
    inputs = ["nums"]
    outputs = ["results"]
    def run(self, nums: ListObjectCollection, results: ListObjectCollection):
        # ... logic ...
        pass

define_process(MyProcess, "my_proc", inputs={"nums": oc1}, outputs={"results": oc2})

# Start the scheduler
get_runtime().start()
```
