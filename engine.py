from abc import ABC, abstractmethod
from enum import Enum, auto
import logging
import sys
import pandas as pd
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)

logger = logging.getLogger(__name__)


class Modes(Enum):
    LIVE = auto()
    REPLAY = auto()


class DataHandler(ABC):

    @abstractmethod
    def get_next_bar(self):
        pass


class ReplayDataHandler(DataHandler):

    def __init__(self):
        try:
            files = [f for f in os.listdir("csv_port") if f.endswith(".csv")]
            if len(files) != 1:
                raise FileNotFoundError(f"Expected 1 CSV, found {len(files)}.")
            self.path_to_csv = os.path.join("csv_port", files[0])
            logger.info(f"Connecting to CSV: {self.path_to_csv}")

            self.data_iterator = pd.read_csv(
                self.path_to_csv,
                usecols=[
                    "ts_event",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "symbol",
                ],
                dtype={
                    "open": int,
                    "high": int,
                    "low": int,
                    "close": int,
                    "volume": int,
                    "symbol": str,
                },
                iterator=True,
                chunksize=1,
            )
        except Exception as e:
            logger.critical(f"Error: {e}", exc_info=False)
            sys.exit(1)

    def get_next_bar(self):
        try:
            row = next(self.data_iterator)

            return {
                "ts_event": pd.to_datetime(row["ts_event"].values[0], unit="ns"),
                "open": row["open"].values[0] / 1e9,
                "high": row["high"].values[0] / 1e9,
                "low": row["low"].values[0] / 1e9,
                "close": row["close"].values[0] / 1e9,
                "volume": row["volume"].values[0],
                "symbol": str(row["symbol"].values[0]),  # Ensure `symbol` is a string
            }
        except StopIteration:
            logger.info("End of data reached.")
            return None
        except Exception as e:
            logger.error(f"Error reading next bar: {e}", exc_info=False)
            return None


class TradingEngine:

    def __init__(self, *, mode: Modes):
        if mode not in Modes:
            logger.error(
                f"Invalid mode: {mode}. Supported: {', '.join(m.name for m in Modes)}"
            )
            sys.exit(1)
        logger.info(f"Trading Engine started in {mode.name} mode.")
        self.mode = mode

        try:
            if self.mode == Modes.LIVE:
                raise NotImplementedError(f"Mode {self.mode} is not implemented.")
            elif self.mode == Modes.REPLAY:
                self.data_handler = ReplayDataHandler()
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=False)
            sys.exit(1)

    def connect(self, *, precalc_window: int = 200):
        """
        Connect to market data and precalculate indicators. The user needs to be aware
        of the amount of bars needed to get a value for all her indicators and specify
        this value as a parameter (`precalc_window`) to this method.
        """
        pass

    def run(self):
        pass
