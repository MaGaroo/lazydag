# LazyDAG Example
## Overview
In this project, we have a queue of integers.
Sometimes, a number is added to the queue.
Sometimes, the first number is removed from the queue.

Then, we want to split the existing numbers of the queue based on their parity.

Later, for the even numbers, we want to divide them by 2.

## Processes
So, we have 3 processes:
- `RandomNumberGeneratorProcess`: Generates random numbers and puts them in the queue.
- `FilterProcess`: Splits the numbers based on their parity.
- `MapProcess`: Divides the even numbers by 2.

For convenience, we also have a `PrintProcess` that prints the numbers in the queue.

These processes are defined in `processes.py`.

## Objects
We have 4 lists: the numbers in the queue, the even numbers,
the odd numbers, and the even numbers divided by 2.

How do we store them? We use 4 lazydag objects for them:
- `rand_nums`: The numbers in the queue.
- `evens`: The even numbers.
- `odds`: The odd numbers.
- `results`: The even numbers divided by 2.

## Topology
Lazydag has to somehow know the topology of our pipeline.
This is defined in `topology.yaml`.

## Instantiation
The processes and objects are instantiated in `pipeline.py`.
Lazydag will read two lists from this file: `processes` and `objects`.
Any other variable in this file will be ignored.
The mappings between the *things* in `pipeline.py` and the *things* in
`topology.yaml` is handled through their `name` attributes.

## How to run?
```bash
# Install lazydag
pip install lazydag

# Initiate the project
lazydag topology from-yaml topology.yaml

# Make sure the topology is valid
lazydag topology validate

# Run the pipeline
lazydag run run
```

Everytime one of the 4 aforementionned lists changes, the print process will
print the list to the console.
