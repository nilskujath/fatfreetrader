from engine import *
from time import sleep

engine = TradingEngine(mode=Modes.REPLAY, symbol="MNQZ4")


bar_1 = engine.data_handler.get_next_bar()
bar_2 = engine.data_handler.get_next_bar()
print(bar_1)
print(bar_2)
engine.add_indicator(SimpleMovingAverage(period=5, applied_on="close"))
engine.add_indicator(SimpleMovingAverage(period=10, applied_on="close"))
engine.connect()
sleep(0.1)
engine.stop()
