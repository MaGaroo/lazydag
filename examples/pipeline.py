from lazydag.schematic import define_process, define_collection
from lazydag.scheduler import get_runtime
from lazydag.collections import ListObjectCollection

# These classes are defined by the user
from processes import MapProcess, FilterProcess, RandomNumberGeneratorProcess, PrintProcess

# Use local paths for testing
oc1 = define_collection(ListObjectCollection, name="Random Numbers", save_path="./data/randoms")
oc2 = define_collection(ListObjectCollection, name="Even Numbers", save_path="./data/evens")
oc3 = define_collection(ListObjectCollection, name="Odd Numbers", save_path="./data/odds")
oc4 = define_collection(ListObjectCollection, name="Processed Even Numbers", save_path="./data/results")

define_process(RandomNumberGeneratorProcess, name='random gen', inputs={}, outputs={"num_list": oc1})
define_process(FilterProcess, name='filter numbers', inputs={"input_nums": oc1}, outputs={"even_nums": oc2, "odd_nums": oc3})
define_process(MapProcess, name='map numbers', inputs={"input_nums": oc2}, outputs={"output_nums": oc4})
define_process(PrintProcess, name='print numbers', inputs={"input_nums": oc1})
define_process(PrintProcess, name='print evens', inputs={"input_nums": oc2})
define_process(PrintProcess, name='print odds', inputs={"input_nums": oc3})
define_process(PrintProcess, name='print results', inputs={"input_nums": oc4})


get_runtime().start()
os._exit(0)
