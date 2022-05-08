def is_bearish(candle):
    return float(candle['k']['c']) < float(candle['k']['o'])

# -------------------------------------------------------------------------------------

def is_bullish(candle):
   return float(candle['k']['c']) > float(candle['k']['o'])

# -------------------------------------------------------------------------------------