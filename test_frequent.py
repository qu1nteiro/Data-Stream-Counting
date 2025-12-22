import sys
import os
import time

# Add current directory to path to locate the 'src' package
sys.path.append(os.getcwd())

from src.data_loader import DatasetLoader
from src.algorithms.exact_counter import ExactCounter
from src.algorithms.frequent_counter import FrequentCounter

# Configuration Constants
DATASET_PATH = "data/amazon_prime_titles.csv"
BUFFER_CAPACITY = 2000  # Small capacity to force the algorithm to "stress" and evict items
DISPLAY_TOP_N = 15


def run_test():
    print(f"--- Testing Frequent-Count Algorithm (Misra-Gries) ---")
    print(f"Dataset: {DATASET_PATH}")
    print(f"Buffer Capacity (k): {BUFFER_CAPACITY}")
    print(f"Note: With k={BUFFER_CAPACITY}, we only track {BUFFER_CAPACITY} items simultaneously.")

    try:
        loader = DatasetLoader(DATASET_PATH)

        # 1. Compute Ground Truth (Exact)
        print("\n1. Computing Exact Counts (Ground Truth)...")
        exact = ExactCounter()

        # We need to consume the stream once for the exact counter
        for actor in loader.stream_actors():
            exact.process(actor)

        # Get the real Top N to compare against
        top_exact = exact.get_top_n(DISPLAY_TOP_N)
        print("   -> Ground Truth computed.")

        # 2. Run Frequent-Count Algorithm
        print("\n2. Running Frequent-Count Algorithm...")
        frequent = FrequentCounter(capacity=BUFFER_CAPACITY)

        start_time = time.time()
        # Create a new stream generator for the second pass
        for actor in loader.stream_actors():
            frequent.process(actor)
        end_time = time.time()

        print(f"   -> Frequent-Count processing finished in {end_time - start_time:.4f}s")

        # 3. Comparative Analysis
        print(f"\n--- COMPARISON: Top {DISPLAY_TOP_N} Actors (Real vs Frequent-Count) ---")
        print(f"{'Rank':<5} | {'Actor':<30} | {'Real':<10} | {'Frequent':<10} | {'Lost Counts':<12}")
        print("-" * 80)

        # We iterate through the REAL top list to see how well Frequent-Count tracked them
        for rank, (actor, real_count) in enumerate(top_exact, 1):
            # Query the approximate algorithm
            est_count = frequent.query(actor)

            # Calculate how many counts were "lost" due to buffer decrements
            lost = real_count - est_count

            # Formatting for clarity
            print(f"#{rank:<4} | {actor:<30} | {real_count:<10} | {est_count:<10} | {lost:<10}")

        # 4. Technical Insight
        print("\n[Technical Analysis]")
        print("1. Underestimation: Notice that 'Frequent' <= 'Real'. This is a guarantee of the algorithm.")
        print("2. The 'Lost Counts' represent the number of times the counter was decremented.")
        print(f"3. Guarantee: If an item appears > N / (k+1) times, it MUST be in the map.")

    except FileNotFoundError:
        print(f"Error: Dataset not found at {DATASET_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    run_test()