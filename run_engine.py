from engine import TradingEngine, Modes

engine = TradingEngine(mode=Modes.REPLAY, symbol="MNQZ4")


bar_1 = engine.data_handler.get_next_bar()
bar_2 = engine.data_handler.get_next_bar()
print(bar_1)
print(bar_2)
engine.connect()
engine.trade(precalc_window=200)
