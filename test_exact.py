# test_exact.py
import sys
import os
import time

# Garante que a raiz está no path
sys.path.append(os.getcwd())

from src.data_loader import DatasetLoader
# Nota o caminho completo agora: src.algorithms.exact_counter
from src.algorithms.exact_counter import ExactCounter


def run_test():
    # Caminho ajustado para a pasta data/
    csv_path = "data/amazon_prime_titles.csv"

    print(f"--- A testar ExactCounter com {csv_path} ---")

    try:
        loader = DatasetLoader(csv_path)
        counter = ExactCounter()

        count = 0
        print("Processing stream...")
        start_time = time.time()
        for actor in loader.stream_actors():
            counter.process(actor)
            count += 1
        end_time = time.time()

        print(f"\nExact Counter ended in {end_time - start_time:.4f}s. Total: {count}")

        print("\n--- TOP 10 ACTORS (Exact) ---")
        top_10 = counter.get_top_n(15)
        for rank, (actor, qtd) in enumerate(top_10, 1):
            print(f"#{rank}: {actor} ({qtd} papéis)")

    except Exception as e:
        print(f"Erro: {e}")


if __name__ == "__main__":
    run_test()