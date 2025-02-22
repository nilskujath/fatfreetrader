from engine import TradingEngine, Modes

engine = TradingEngine(mode=Modes.REPLAY)
engine.connect(precalc_window=200)
