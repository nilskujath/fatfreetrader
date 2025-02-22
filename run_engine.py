from engine import TradingEngine, Modes
from time import sleep

engine = TradingEngine(mode=Modes.REPLAY, symbol="MNQZ4")


bar_1 = engine.data_handler.get_next_bar()
bar_2 = engine.data_handler.get_next_bar()
print(bar_1)
print(bar_2)
engine.connect()
engine.trade()
sleep(5)
engine.stop()
