from lazydag.core.misc import get_processes_and_objects


def get_process_by_name(process_name: str):
    processes, _ = get_processes_and_objects()
    for proc in processes:
        if proc.name == process_name:
            return proc
    raise ValueError(f"Process {process_name} does not exist")

def get_object_by_name(object_name: str):
    _, objects = get_processes_and_objects()
    for obj in objects:
        if obj.name == object_name:
            return obj
    raise ValueError(f"Object {object_name} does not exist")
