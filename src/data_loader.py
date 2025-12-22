import csv
from pathlib import Path
from typing import Generator

# Constants for configuration
CAST_COLUMN_NAME = 'cast'
CSV_DELIMITER = ','
LIST_SEPARATOR = ','
ENCODING = 'utf-8'

# Blacklist semântica (Horses)
# Nomes devem estar em lowercase para comparação
BLOCKED_NAMES = {
    'champion',  # Gene Autry Horse
    'trigger',   # Roy Rogers Horse
    'narrator',  # two times appearance
    'ph.d.',     # Academic Title appearing as actor
    'phd',       # Variation
}

class DatasetLoader:
    """
    Handles the loading and preprocessing of the Amazon Prime dataset.
    Designed to behave like a data stream provider.
    """

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"Dataset not found at: {self.file_path}")

    def stream_actors(self) -> Generator[str, None, None]:
        """
        Yields individual actor names one by one.
        FILTERS:
        - Removes empty strings.
        - Removes pure numbers (e.g., "1", "2020") that existed.
        - Removes single characters.
        """
        try:
            with self.file_path.open(mode='r', encoding=ENCODING, newline='') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=CSV_DELIMITER)

                for row in reader:
                    cast_raw = row.get(CAST_COLUMN_NAME)
                    if not cast_raw:
                        continue

                    actors = cast_raw.split(LIST_SEPARATOR)

                    for actor in actors:
                        clean_name = actor.strip()
                        name_lower = clean_name.lower()

                        # 1. Ignore empty
                        if not clean_name:
                            continue

                        # 2. Ignore numbers
                        if clean_name.isdigit():
                            continue

                        # 3. Ignore single characters
                        if len(clean_name) < 2:
                            continue

                        #4. Ignore names in blacklist
                        if name_lower in BLOCKED_NAMES:
                            continue

                        yield clean_name

        except csv.Error as e:
            print(f"Error reading CSV file: {e}")
            raise


if __name__ == "__main__":
    # Quick verification
    TEST_PATH = "../data/amazon_prime_titles.csv"
    print(f"Testing simplified loader on: {TEST_PATH}")

    loader = DatasetLoader(TEST_PATH)
    count = 0
    for actor in loader.stream_actors():
        count += 1
        # Print just the first few to ensure formatting is correct
        if count <= 3:
            print(f"Actor: '{actor}'")

    print(f"Loader working. Total streams processed: {count}")

    #Looks correct, REMINDER FOR ME -- TRY WITH PANDAS LATER --