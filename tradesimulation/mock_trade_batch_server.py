import random
import datetime
import time
import json

# --- Configuration for mock data ---
TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']
TRADE_TYPES = ['BUY', 'SELL']
MAX_BATCH_SIZE = 500  # Generate up to 500 trades per cycle
MIN_BATCH_SIZE = 100
SLEEP_TIME_SECONDS = 1  # Wait time between generation cycles (1 second)


def generate_mock_trade():
    """Generates a single, highly randomized trade dictionary."""
    ticker = random.choice(TICKERS)
    trade_type = random.choice(TRADE_TYPES)

    # Prices change slightly based on the previous price (not implemented here for simplicity)
    # Using a high range for volume simulation
    price = round(random.uniform(10.00, 2000.00), 2)
    quantity = random.randint(100, 5000)  # Increased quantity for "huge volumes"

    # Use high-precision timestamp
    now = datetime.datetime.now()
    timestamp = now.isoformat()

    # Generate a unique ID
    trade_id = now.strftime("%Y%m%d%H%M%S") + str(now.microsecond) + str(random.randint(1000, 9999))

    return {
        'trade_id': trade_id,
        'ticker': ticker,
        'type': trade_type,
        'price': price,
        'quantity': quantity,
        'timestamp': timestamp,
        'notional_value': round(price * quantity, 2)
    }


def continuous_generation():
    """Generates trades continuously and prints them."""
    total_trades_generated = 0
    start_time = time.time()

    print("--- 🚀 Starting High-Frequency Trade Data Generation (Printing ALL Trades) ---")

    try:
        while True:
            # Determine a random batch size to simulate variable trade activity
            num_trades = random.randint(MIN_BATCH_SIZE, MAX_BATCH_SIZE)

            # Generate the batch of trades
            trades_batch = [generate_mock_trade() for _ in range(num_trades)]

            # --- MODIFIED OUTPUT SIMULATION ---

            # Print the header for the batch
            print(f"\n--- BATCH START ({len(trades_batch)} trades) ---")

            # Loop through the batch and print each trade individually
            for trade in trades_batch:
                # Use json.dumps to format the dictionary nicely on a single line
                print(json.dumps(trade))

            # Print the footer for the batch
            print("--- BATCH END ---\n")

            total_trades_generated += num_trades

            # Wait a short time to simulate frequency control
            time.sleep(SLEEP_TIME_SECONDS)

            # Periodically print the running count and trades per second (TPS)
            if total_trades_generated % 5000 < MAX_BATCH_SIZE:
                elapsed_time = time.time() - start_time
                tps = total_trades_generated / elapsed_time if elapsed_time > 0 else 0
                print(
                    f"\n[STAT] Total trades: **{total_trades_generated:,}** | Run time: {elapsed_time:.2f}s | **Rate: {tps:,.0f} TPS**\n")

    except KeyboardInterrupt:
        elapsed_time = time.time() - start_time
        tps = total_trades_generated / elapsed_time if elapsed_time > 0 else 0
        print(f"\n--- 🛑 Simulation Stopped ---")
        print(f"Final Count: {total_trades_generated:,} trades.")
        print(f"Average Rate: {tps:,.0f} Trades Per Second (TPS).")

if __name__ == '__main__':
    continuous_generation()