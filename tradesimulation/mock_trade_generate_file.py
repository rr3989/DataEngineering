import json
import time
import random
from datetime import datetime, timedelta


def generate_trade_data(trade_id, version):

    current_time = datetime.now()
    timestamp_str = current_time.isoformat(timespec='microseconds')

    future_days = random.randint(1, 365)
    maturity_date = (current_time + timedelta(days=future_days)).strftime('%Y-%m-%d')

    symbols = ['AAPL', 'GOOGL', 'MSFT', 'META', 'BK', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']
    clients = ['ClientA', 'ClientB', 'ClientC', 'ClientD']

    trade = {
        "trade_id": str(trade_id),
        "version": version,
        "client_id": random.choice(clients),
        "symbol": random.choice(symbols),
        "side": random.choice(['Buy','Sell']),
        "price": round(random.uniform(50.0, 1500.0), 2),
        "quantity": random.randint(10, 1000),
        "maturity_date": maturity_date,
        "timestamp": timestamp_str,
    }
    return trade


def generate_and_save_trades(output_file, num_trades):
    trade_versions = {}

    print(f"Starting trade generation: {num_trades} total trades to create...")

    with open(output_file, 'w') as f:
        for i in range(1, num_trades + 1):

            if random.random() < 0.7 or not trade_versions:
                current_trade_id = 100000 + i
                trade_versions[current_trade_id] = 1  # Start at Version 1
            else:
                current_trade_id = random.choice(list(trade_versions.keys()))
                trade_versions[current_trade_id] += 1  # Increment the version

            version = trade_versions[current_trade_id]
            trade_payload = generate_trade_data(current_trade_id, version)
            json_line = json.dumps(trade_payload)
            f.write(json_line + '\n')

            if i % 1000 == 0:
                print(f"Generated {i} trades...")

    print(f"\n✅ Finished generating {num_trades} trades to {output_file}")
    print(f"Unique Trade IDs created: {len(trade_versions)}")


if __name__ == '__main__':
    OUTPUT_FILE_PATH = "local_trade_events.json"
    NUMBER_OF_TRADES = 10000

    generate_and_save_trades(OUTPUT_FILE_PATH, NUMBER_OF_TRADES)