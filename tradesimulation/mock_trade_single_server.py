import random
import datetime
import time
import json

# --- Configuration for mock data ---
TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'META', 'BK', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']
TRADE_TYPES = ['BUY', 'SELL']
CURRENCIES = ['USD', 'EUR', 'GBP']
EXCHANGES = ['NASDAQ', 'NYSE', 'LSE']
SLEEP_TIME_SECONDS = 0.01  # High frequency (10 milliseconds)

# A small list of clients/accounts to randomly assign to trades
MOCK_CLIENTS = [
    {'client_id': 'C1001', 'name': 'Alpha Investments', 'account_id': 'A5005'},
    {'client_id': 'C1002', 'name': 'Beta Hedge Fund', 'account_id': 'B6006'},
    {'client_id': 'C1003', 'name': 'Retail Trader Fund', 'account_id': 'R7007'},
    {'client_id': 'C1004', 'name': 'Gamma Wealth Mgmt', 'account_id': 'G8008'},
    {'client_id': 'C1005', 'name': 'Individual Investor Fund', 'account_id': 'I9009'},
]

def generate_mock_trade():
    """Generates a single, highly randomized trade dictionary with client/market context."""

    # Randomly select client/account info from the pre-defined list
    client_info = random.choice(MOCK_CLIENTS)
    ticker = random.choice(TICKERS)
    trade_type = random.choice(TRADE_TYPES)
    currency = random.choice(CURRENCIES)
    exchange = random.choice(EXCHANGES)

    # Price and Quantity generation
    price = round(random.uniform(10.00, 2000.00), 2)
    quantity = random.randint(100, 5000)

    # High-precision timestamp and unique ID
    now = datetime.datetime.now()
    timestamp = now.isoformat()
    trade_id = now.strftime("%Y%m%d%H%M%S") + str(now.microsecond) + str(random.randint(100, 999))

    return {
        'trade_id': trade_id,
        'client_id': client_info['client_id'],
        'client_name': client_info['name'],
        'account_id': client_info['account_id'],
        'instrument': {
            'ticker': ticker,
            'exchange': exchange,
            'currency': currency
        },
        'transaction': {
            'type': trade_type,
            'price': price,
            'quantity': quantity,
            'notional_value': round(price * quantity, 2)
        },
        'timestamp': timestamp
    }


def continuous_generation():
    """Generates one trade at a time continuously and prints it."""
    total_trades_generated = 0
    start_time = time.time()

    print("--- 🚀 Starting Enhanced Trade Stream (Client/Market Data Included) ---")

    try:
        while True:
            trade = generate_mock_trade()

            # Print the single trade immediately as a JSON string
            print(json.dumps(trade))

            total_trades_generated += 1

            time.sleep(SLEEP_TIME_SECONDS)

            # Periodically print the running count and Trades Per Second (TPS)
            if total_trades_generated % 100 == 0:
                elapsed_time = time.time() - start_time
                tps = total_trades_generated / elapsed_time if elapsed_time > 0 else 0
                print(
                    "[STAT] Total trades: {total_trades_generated:,} | Run time: {elapsed_time:.2f}s | **Rate: {tps:,.0f} TPS**")

    except KeyboardInterrupt:
        elapsed_time = time.time() - start_time
        tps = total_trades_generated / elapsed_time if elapsed_time > 0 else 0
        # Printing FINAL STATS to stderr
        print(f"\n--- 🛑 Simulation Stopped ---")
        print(f"Final Count: {total_trades_generated:,} trades.")
        print(f"Average Rate: {tps:,.0f} Trades Per Second (TPS).")


if __name__ == '__main__':
    continuous_generation()