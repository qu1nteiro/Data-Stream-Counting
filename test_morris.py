import sys
import os
import time

# Adicionar o diretório atual ao path para encontrar o pacote 'src'
sys.path.append(os.getcwd())

from src.data_loader import DatasetLoader
from src.algorithms.exact_counter import ExactCounter
from src.algorithms.morris_counter import MorrisCounter


def run_test():
    csv_path = "data/amazon_prime_titles.csv"
    print(f"--- Testing Morris Counter with {csv_path} ---")

    try:
        loader = DatasetLoader(csv_path)

        # 1. Obter a 'Ground Truth' (Verdade Absoluta)
        # Precisamos disto para saber se o Morris está a mentir ou não.
        print("1. Calculating exact counters (Baseline)...")
        exact = ExactCounter()
        # Consumimos o stream uma vez
        for actor in loader.stream_actors():
            exact.process(actor)

        top_exact = exact.get_top_n(15)
        print("   -> Baseline calculated.")

        # 2. Executar o Morris Counter
        print("2. Morris Counter executing now (1/2^k)...")
        morris = MorrisCounter()

        # Recarregar o stream (criar novo gerador)
        start_time = time.time()
        for actor in loader.stream_actors():
            morris.process(actor)
        end_time = time.time()

        print(f"   -> Morris ended in {end_time - start_time:.4f}s")

        # 3. Comparação Lado a Lado
        print("\n--- COMPARISON: Top 10 Actors (Real vs Estimated) ---")
        print(f"{'Rank':<5} | {'Actor (Exact)':<30} | {'Real':<10} | {'Morris (Est)':<15} | {'Rel Error':<10}")
        print("-" * 85)

        # Vamos iterar sobre o Top 10 REAL e ver quanto o Morris marcou para eles
        for rank, (actor, real_count) in enumerate(top_exact, 1):
            est_count = morris.query(actor)

            # Cálculo do erro relativo: |est - real| / real
            if real_count > 0:
                error = abs(est_count - real_count) / real_count * 100
            else:
                error = 0.0

            print(f"#{rank:<4} | {actor:<30} | {real_count:<10} | {est_count:<15} | {error:>6.1f}%")

        print("\nNote:")
        print("Morris Counter estimates n = 2^k - 1.")
        print("The possible values are: 1, 3, 7, 15, 31, 63, 127...")

    except FileNotFoundError:
        print("Error: File CSV not found.")
    except Exception as e:
        print(f"Not known error: {e}")


if __name__ == "__main__":
    run_test()