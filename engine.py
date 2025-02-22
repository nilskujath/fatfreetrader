from abc import ABC, abstractmethod
from enum import Enum, auto
import logging
import sys


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

    def connect(self):
        pass

    def get_next_bar(self):
        pass


class TradingEngine:

    def __init__(self, mode: Modes):
        if mode not in Modes:
            logger.critical(
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
            logger.critical(f"Error: {e}", exc_info=True)
            sys.exit(1)

    def run(self):
        pass


if __name__ == "__main__":
    engine = TradingEngine(Modes.REPLAY)
    engine.run()
