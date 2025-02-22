import os
import sys
import pandas as pd
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)

logger = logging.getLogger(__name__)


class FatFreeEngine:
    def connect(self):
        try:
            files = [f for f in os.listdir("csv_port") if f.endswith(".csv")]
            if len(files) != 1:
                raise FileNotFoundError(f"Expected 1 CSV, found {len(files)}.")

            df = pd.read_csv(file_path := os.path.join("csv_port", files[0]))
            logger.info(f"Loaded CSV: {file_path}, Shape: {df.shape}")
            print(df.head())

        except Exception as e:
            logger.critical(f"Error: {e}", exc_info=True)
            sys.exit(1)
