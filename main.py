import sys
import os
import time
import statistics
from typing import List, Dict, Tuple, Any

sys.path.append(os.getcwd())

from src.data_loader import DatasetLoader
from src.algorithms.exact_counter import ExactCounter
from src.algorithms.morris_counter import MorrisCounter
from src.algorithms.frequent_counter import FrequentCounter

# GLOBAL CONFIGURATION
DATASET_PATH: str = "data/amazon_prime_titles.csv"
MORRIS_TRIALS: int = 20  # Number of runs to smooth out variance
FREQUENT_CAPACITIES: List[int] = [100, 500, 1000, 2500, 3500, 44000]  # Different 'k' values for memory testing
DISPLAY_TOP_N: int = 15  # Number of top items to display in reports


def print_section_header(title: str) -> None:
    """
    Utility function to print formatted section headers for the report.
    """
    print(f"\n{'=' * 100}")
    print(f" {title}")
    print(f"{'=' * 100}")


def run_exact_benchmark(loader: DatasetLoader) -> Tuple[List[Tuple[str, int]], Dict[str, int]]:
    """
    Phase 1: Computes the exact result using the ExactCounter.

    Returns:
        A tuple containing:
        1. List of Top N items (ranking).
        2. A Dictionary mapping 'Actor' -> 'Exact Count' for O(1) lookups later.
    """
    print(">>> PHASE 1: Computing Ground Truth (Exact Counter)...")

    exact_counter = ExactCounter()

    # Measure execution time for the baseline
    start_time = time.time()
    for actor in loader.stream_actors():
        exact_counter.process(actor)
    elapsed_time = time.time() - start_time

    print(f"    Baseline Execution Time: {elapsed_time:.4f} seconds")

    # Retrieve top N for analysis
    top_n_list = exact_counter.get_top_n(DISPLAY_TOP_N)

    # Create a hash map for fast comparisons in subsequent phases
    exact_map = {name: count for name, count in top_n_list}

    return top_n_list, exact_map


def run_morris_experiment(loader: DatasetLoader,
                          top_exact: List[Tuple[str, int]],
                          exact_map: Dict[str, int]) -> None:
    """
    Phase 2: Evaluates the Probabilistic Counter (Morris Algorithm).
    Executes the algorithm multiple times to analyze variance and average error.
    """
    print_section_header(f"PHASE 2: Probabilistic Counter Analysis (Avg of {MORRIS_TRIALS} runs)")

    # Data structure to store results: { "ActorName": [est_run_1, est_run_2, ...] }
    morris_results: Dict[str, List[int]] = {name: [] for name in exact_map}

    print(f"    Executing {MORRIS_TRIALS} Monte Carlo simulations", end="", flush=True)

    for _ in range(MORRIS_TRIALS):
        # Instantiate a new counter for each independent trial
        morris = MorrisCounter()

        # Process the full stream
        for actor in loader.stream_actors():
            morris.process(actor)

        # Record estimates only for the actors we are interested in (the true Top N)
        for name in exact_map:
            estimate = morris.query(name)
            morris_results[name].append(estimate)

        # Visual progress indicator
        print(".", end="", flush=True)

    print(" Done.")

    # --- Generate Report Table ---
    print("\n[Table 1] Accuracy of Approximate Counter (Morris 1/2^k)")
    print(
        f"{'Rank':<4} | {'Actor':<25} | {'Real':<6} | {'Avg (Est)':<12} | {'Rel Err %':<10} | {'Min':<6} | {'Max':<6}")
    print("-" * 90)

    for rank, (name, real_count) in enumerate(top_exact, 1):
        estimates = morris_results[name]

        # Statistical analysis
        avg_est = statistics.mean(estimates)
        min_est = min(estimates)
        max_est = max(estimates)

        # Calculate Relative Error based on the Average
        if real_count > 0:
            rel_error = (abs(avg_est - real_count) / real_count) * 100
        else:
            rel_error = 0.0

        print(
            f"#{rank:<3} | {name:<25} | {real_count:<6} | {avg_est:>10.1f}   | {rel_error:>9.1f}% | {min_est:<6} | {max_est:<6}")


def run_frequent_experiment(loader: DatasetLoader,
                            top_exact: List[Tuple[str, int]]) -> None:
    """
    Phase 3: Evaluates the Frequent-Count Algorithm.
    Tests the algorithm's sensitivity to buffer memory size (k).
    """
    print_section_header("PHASE 3: Frequent-Count Sensitivity to Memory Capacity (k)")

    print("[Table 2] Impact of Buffer Size on Accuracy (Underestimation Analysis)")

    # 1. Build the header for the table
    header = f"{'Rank':<4} | {'Actor':<25} | {'Real':<6}"
    for cap in FREQUENT_CAPACITIES:
        header += f" | {'k=' + str(cap):<8}"

    print(header)
    print("-" * len(header))

    # 2. Run the algorithm for each defined capacity
    # We store the resulting counter objects in a dictionary for querying
    results_by_capacity: Dict[int, FrequentCounter] = {}

    for cap in FREQUENT_CAPACITIES:
        # Initialize with specific capacity
        fc_algo = FrequentCounter(capacity=cap)

        # Process stream
        for actor in loader.stream_actors():
            fc_algo.process(actor)

        results_by_capacity[cap] = fc_algo

    # 3. Print the comparative rows
    for rank, (name, real_count) in enumerate(top_exact, 1):
        row_str = f"#{rank:<3} | {name:<25} | {real_count:<6}"

        for cap in FREQUENT_CAPACITIES:
            estimated_count = results_by_capacity[cap].query(name)

            # formatting: if count is 0, show a hyphen for readability
            val_str = str(estimated_count) if estimated_count > 0 else "-"
            row_str += f" | {val_str:<8}"

        print(row_str)


def main() -> None:
    """
    Main Orchestrator.
    Coordinates the data loading and the execution of all experimental phases.
    """
    print_section_header("STARTING SCIENTIFIC EXPERIMENT - ADVANCED ALGORITHMS")

    # Initialize the Data Stream Provider
    try:
        loader = DatasetLoader(DATASET_PATH)
    except FileNotFoundError:
        print(f"CRITICAL ERROR: Dataset not found at '{DATASET_PATH}'. Aborting.")
        return

    # Phase 1: Establish Baseline
    top_exact, exact_map = run_exact_benchmark(loader)

    # Phase 2: Probabilistic Analysis
    run_morris_experiment(loader, top_exact, exact_map)

    # Phase 3: Streaming/Frequent Item Analysis
    run_frequent_experiment(loader, top_exact)

    print("\n" + "=" * 100)
    print("EXPERIMENT COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()