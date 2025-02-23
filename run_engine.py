from engine import *
from time import sleep

engine = TradingEngine(mode=Modes.REPLAY, symbol="MNQZ4")

engine.add_indicator(SimpleMovingAverage(period=5, applied_on="close"))
engine.add_indicator(SimpleMovingAverage(period=10, applied_on="close"))

engine.connect()

sleep(0.1)
engine.stop()

