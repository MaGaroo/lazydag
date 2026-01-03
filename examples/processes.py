from lazydag.contrib.objects import FSListObject
from lazydag.core.process import Process
import queue
import random
import time

class RandomNumberGeneratorProcess(Process):
    inputs = []
    outputs = ["num_list"]
    has_daemon = True

    def __init__(self, name):
        super().__init__(name)
        self.queue = queue.Queue()

    def on_pipeline_start(self):
        self._run_daemon = True

    def on_pipeline_end(self):
        self._run_daemon = False

    def run_daemon(self, num_list: FSListObject):
        # Generate some numbers
        while self._run_daemon:
            val = random.randint(1, 100)
            self.queue.put(val)
            time.sleep(0.5)

    def poll(self, num_list: FSListObject):
        # Move items from queue to output object
        while not self.queue.empty():
            try:
                val = self.queue.get_nowait()
                num_list.push(val)
                if len(num_list) > 10:
                    num_list.remove(0)
            except queue.Empty:
                break

class FilterProcess(Process):
    inputs = ["input_nums"]
    outputs = ["even_nums", "odd_nums"]

    def __init__(self, name):
        super().__init__(name)
        self.processed_idx = 0

    def poll(self, input_nums: FSListObject, even_nums: FSListObject, odd_nums: FSListObject):
        if not input_nums.changed():
            return
        # Process new items from input_nums
        even_ptr = 0
        odd_ptr = 0
        for num in input_nums:
            if num % 2 == 0:
                if even_ptr < len(even_nums):
                    even_nums.set(even_ptr, num)
                else:
                    even_nums.push(num)
                even_ptr += 1
            else:
                if odd_ptr < len(odd_nums):
                    odd_nums.set(odd_ptr, num)
                else:
                    odd_nums.push(num)
                odd_ptr += 1
        while len(even_nums) > even_ptr:
            even_nums.remove(even_ptr)
        while len(odd_nums) > odd_ptr:
            odd_nums.remove(odd_ptr)

class MapProcess(Process):
    inputs = ["input_nums"]
    outputs = ["output_nums"]

    def __init__(self, name):
        super().__init__(name)
        self.processed_idx = 0

    def poll(self, input_nums, output_nums):
        if not input_nums.changed():
            return
        ptr = 0
        for num in input_nums:
            if ptr < len(output_nums):
                output_nums.set(ptr, num // 2)
            else:
                output_nums.push(num // 2)
            ptr += 1
        while len(output_nums) > ptr:
            output_nums.remove(ptr)


class PrintProcess(Process):
    inputs = ["input_nums"]
    outputs = []

    def __init__(self, name):
        super().__init__(name)
        self.processed_idx = 0

    def poll(self, input_nums):
        if not input_nums.changed():
            return
        print(self.name, ":", ",".join(map(str, input_nums)))
