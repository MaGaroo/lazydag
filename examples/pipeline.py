from lazydag.contrib.objects import FSListObject
from pathlib import Path

# These classes are defined by the user
from processes import MapProcess, FilterProcess, RandomNumberGeneratorProcess, PrintProcess

# Use local paths for testing
rand_nums = FSListObject(name="rand_nums")
evens = FSListObject(name="evens")
odds = FSListObject(name="odds")
results = FSListObject(name="results")

random_gen_process = RandomNumberGeneratorProcess(name='random_gen')
filter_process = FilterProcess(name='filter_numbers')
map_process = MapProcess(name='map_numbers')
print_process = PrintProcess(name='print_numbers')
print_evens_process = PrintProcess(name='print_evens')
print_odds_process = PrintProcess(name='print_odds')
print_results_process = PrintProcess(name='print_results')

processes = [
    random_gen_process,
    filter_process,
    map_process,
    print_process,
    print_evens_process,
    print_odds_process,
    print_results_process,
]
objects = [
    rand_nums,
    evens,
    odds,
    results,
]
