from lazydag.collections import ListObjectCollection

# These classes are defined by the user
from processes import MapProcess, FilterProcess, RandomNumberGeneratorProcess, PrintProcess

# Use local paths for testing
rand_nums = ListObjectCollection(name="rand_nums", save_path="./data/randoms")
evens = ListObjectCollection(name="evens", save_path="./data/evens")
odds = ListObjectCollection(name="odds", save_path="./data/odds")
results = ListObjectCollection(name="results", save_path="./data/results")

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
collections = [
    rand_nums,
    evens,
    odds,
    results,
]
