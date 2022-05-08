def calculate_moving_average(closing_prices , period):
    closing_price_sum = 0
    for close_price in closing_prices[-period:]:
        closing_price_sum += float(close_price)
    
    return float(closing_price_sum / period)

# -------------------------------------------------------------------------------------