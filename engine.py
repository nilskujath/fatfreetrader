from abc import ABC, abstractmethod
from enum import Enum, auto
from dataclasses import dataclass
import logging
import sys
import pandas as pd
import numpy as np
import os
from collections import deque
from queue import Queue, Empty
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


@dataclass
class ProcessedBarEventMessage:
    ts_event: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str

    @classmethod
    def from_bar(cls, bar: BarEventMessage, indicator_values: dict):
        return cls(
            ts_event=bar.ts_event,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            symbol=bar.symbol,
            **indicator_values,
        )


class Modes(Enum):
    LIVE = auto()
    REPLAY = auto()


class Indicator(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def update(self, bar: BarEventMessage):
        pass

    @abstractmethod
    def value(self):
        pass


class SimpleMovingAverage(Indicator):
    def __init__(self, period: int, applied_on: str):
        self.period = period
        self.applied_on = applied_on
        self.values = deque(maxlen=self.period)
        self._current_value = np.nan

    @property
    def name(self) -> str:
        return f"SMA_{self.period}_{self.applied_on}"

    def update(self, bar: BarEventMessage):
        self.values.append(getattr(bar, self.applied_on))
        if len(self.values) == self.period:
            self._current_value = sum(self.values) / self.period

    def value(self):
        return self._current_value


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
        self.mode: Modes = mode
        self.symbol: str = symbol
        self.incoming_market_data_queue = Queue()
        self.processed_market_data_queue = Queue()
        self.indicators: dict = {}
        self._stop_event = threading.Event()
        self._graceful_stop: bool = False

        try:
            if self.mode == Modes.LIVE:
                raise NotImplementedError(f"Mode {self.mode} is not implemented.")
            elif self.mode == Modes.REPLAY:
                self.data_handler = ReplayDataHandler(symbol)
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=False)
            sys.exit(1)

    def add_indicator(self, indicator: Indicator):
        pass

    def connect(self):
        self.fetch_market_data_thread = threading.Thread(target=self._fetch_market_data)
        self.fetch_market_data_thread.start()

        self.process_market_data_thread = threading.Thread(
            target=self._process_market_data
        )
        self.process_market_data_thread.start()

    def _fetch_market_data(self):
        while not self._stop_event.is_set():
            bar = self.data_handler.get_next_bar()
            if bar is None:
                logger.info(f"End of data reached for symbol: {self.symbol}.")
                break
            self.incoming_market_data_queue.put(bar)
            logger.debug(f"Enqueued new bar event: {bar}")

    def _process_market_data(self):
        logger.info("Trade thread started.")
        while not self._stop_event.is_set():
            try:
                bar_event_message = self.incoming_market_data_queue.get(timeout=0.02)

                if self._stop_event.is_set() and not self._graceful_stop:
                    logger.info("Trade thread stopping immediately.")
                    break

                self.incoming_market_data_queue.task_done()
                logger.debug(f"Processed bar event: {bar_event_message}")

            except Empty:
                if self._stop_event.is_set():
                    logger.info("Trade thread exiting due to stop event.")
                    break

    def stop(self, graceful: bool = True):
        logger.info(f"Stopping Trading Engine (graceful={graceful})...")
        self._graceful_stop = graceful
        self._stop_event.set()

        if not graceful:
            while not self.incoming_market_data_queue.empty():
                try:
                    self.incoming_market_data_queue.get_nowait()
                except Empty:
                    break

        self.fetch_market_data_thread.join()
        self.process_market_data_thread.join()
        logger.info("Trading Engine stopped.")
