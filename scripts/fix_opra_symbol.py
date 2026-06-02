# List of your raw stock tickers
tickers = [
    "TSLA", "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "AMD",
    "NFLX", "AVGO", "C", "JPM", "OXY" # ... add the rest of your 100 tickers here
]

# The expiration date (YYMMDD) - April 17, 2026
exp_date = "260417"
opt_type = "C"

# For this test, we will assign a dummy strike of $100.00 to all of them
# (100 * 1000 = 100000 -> padded to 8 digits = 00100000)
strike = "00100000"

print("Perfect 21-Character OCC Symbols:")
for ticker in tickers:
    # .ljust(6) automatically pads the right side with spaces until it reaches 6 characters
    occ_symbol = f"{ticker.ljust(6)}{exp_date}{opt_type}{strike}"

    # Validation check to guarantee it is exactly 21 characters
    assert len(occ_symbol) == 21, f"Failed length check: {occ_symbol}"

    print(occ_symbol)