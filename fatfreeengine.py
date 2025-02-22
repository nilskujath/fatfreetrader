import os
import sys
import pandas as pd
import logging
from enum import Enum, auto

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)

logger = logging.getLogger(__name__)


class Mode(Enum):
    LIVE = auto()
    REPLAY = auto()


class DataHandler:
    pass


class FatFreeEngine:
    def __init__(self, mode: Mode):
        if mode not in Mode:
            logger.critical(
                f"Invalid mode: {mode}. Supported: {', '.join(m.name for m in Mode)}"
            )
            sys.exit(1)
        self.mode = mode

    def connect(self):
        try:
            if self.mode == Mode.LIVE:
                raise NotImplementedError(f"Mode {self.mode} is not implemented.")
            elif self.mode == Mode.REPLAY:
                self._connect_backtest()
            else:
                raise ValueError(
                    f"Invalid mode: {self.mode}. Supported modes: {', '.join(m.name for m in Mode)}"
                )
        except Exception as e:
            logger.critical(f"Error: {e}", exc_info=True)
            sys.exit(1)

    def _connect_backtest(self):
        try:
            files = [f for f in os.listdir("csv_port") if f.endswith(".csv")]
            if len(files) != 1:
                raise FileNotFoundError(f"Expected 1 CSV, found {len(files)}.")

            path_to_csv = os.path.join("csv_port", files[0])
            logger.info(f"Reading CSV: {path_to_csv}")

            with open(path_to_csv, "r") as f:
                df = pd.read_csv(f)

            logger.info(f"Loaded CSV: {path_to_csv}, Shape: {df.shape}")
            print(df.head())

        except Exception as e:
            logger.critical(f"Error: {e}", exc_info=True)
            sys.exit(1)
