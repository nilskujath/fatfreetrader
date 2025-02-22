from abc import ABC, abstractmethod
from enum import Enum, auto
from dataclasses import dataclass
import logging
import sys
import pandas as pd
import os
from collections import deque
from queue import Queue
import threading


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
)

logger = logging.getLogger(__name__)


@dataclass
class BarEventMessage:
    ts_event: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str


class Modes(Enum):
    LIVE = auto()
    REPLAY = auto()


class DataHandler(ABC):

    @abstractmethod
    def get_next_bar(self):
        pass


class ReplayDataHandler(DataHandler):

    def __init__(self, symbol: str):
        try:
            files = [f for f in os.listdir("csv_port") if f.endswith(".csv")]
            if len(files) != 1:
                raise FileNotFoundError(f"Expected 1 CSV, found {len(files)}.")
            self.path_to_csv = os.path.join("csv_port", files[0])
            logger.info(f"Connecting to CSV: {self.path_to_csv}")

            self.symbol = symbol
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

    def get_next_bar(self) -> BarEventMessage | None:
        try:
            while True:
                row = next(self.data_iterator)

                if row["symbol"].values[0] == self.symbol:
                    return BarEventMessage(
                        ts_event=pd.to_datetime(row["ts_event"].values[0], unit="ns"),
                        open=row["open"].values[0] / 1e9,
                        high=row["high"].values[0] / 1e9,
                        low=row["low"].values[0] / 1e9,
                        close=row["close"].values[0] / 1e9,
                        volume=row["volume"].values[0],
                        symbol=row["symbol"].values[0],
                    )
        except StopIteration:
            logger.info(f"End of data reached for symbol: {self.symbol}")
            return None
        except Exception as e:
            logger.error(f"Error reading next bar: {e}", exc_info=False)
            return None


class TradingEngine:

    def __init__(self, *, mode: Modes, symbol: str):
        if mode not in Modes:
            logger.error(
                f"Invalid mode: {mode}. Supported: {', '.join(m.name for m in Modes)}"
            )
            sys.exit(1)
        logger.info(f"Trading Engine started in {mode.name} mode.")
        self.mode = mode
        self.symbol = symbol
        self.incoming_market_data_queue = Queue()
        self._stop_event = threading.Event()

        try:
            if self.mode == Modes.LIVE:
                raise NotImplementedError(f"Mode {self.mode} is not implemented.")
            elif self.mode == Modes.REPLAY:
                self.data_handler = ReplayDataHandler(symbol)
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=False)
            sys.exit(1)

    def connect(self):
        self.fetch_market_data_thread = threading.Thread(target=self._connect)
        self.fetch_market_data_thread.start()

    def _connect(self):
        while not self._stop_event.is_set():
            bar = self.data_handler.get_next_bar()
            if bar is None or self._stop_event.is_set():
                logger.info(f"End of data reached for symbol: {self.symbol}.")
                break
            self.incoming_market_data_queue.put(bar)
            logger.debug(f"Enqueued new bar event: {bar}")

    def trade(self):
        self.trade_thread = threading.Thread(target=self._trade)
        self.trade_thread.start()

    def _trade(self):
        while not self._stop_event.is_set():
            if not self.incoming_market_data_queue.empty():
                bar_event_message: BarEventMessage = (
                    self.incoming_market_data_queue.get()
                )
                print(bar_event_message)

    def stop(self):
        logger.info("Stopping Trading Engine...")
        self._stop_event.set()
        self.fetch_market_data_thread.join()
        self.trade_thread.join()
        logger.info("Trading Engine stopped.")
