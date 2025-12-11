import random
import datetime
import time
import json
import sys
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

# --- mock data ---
TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'META', 'BK', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']
TRADE_TYPES = ['BUY', 'SELL']
CURRENCIES = ['USD', 'EUR', 'GBP']
EXCHANGES = ['NASDAQ', 'NYSE', 'LSE']
SLEEP_TIME_SECONDS = 0.1  # High Frequency

# list of clients/accounts to randomly assign to trades
MOCK_CLIENTS = [
    {'client_id': 'C1001', 'name': 'Alpha Investments', 'account_id': 'A5005'},
    {'client_id': 'C1002', 'name': 'Beta Hedge Fund', 'account_id': 'B6006'},
    {'client_id': 'C1003', 'name': 'Retail Trader Fund', 'account_id': 'R7007'},
    {'client_id': 'C1004', 'name': 'Gamma Wealth Mgmt', 'account_id': 'G8008'},
    {'client_id': 'C1005', 'name': 'Individual Investor Fund', 'account_id': 'I9009'},
]

# --- GCP Pub/Sub Configuration ---
GCP_PROJECT_ID = 'vibrant-mantis-289406'
PUBSUB_TOPIC_ID = 'trade-topics'
PUBSUB_TOPIC_PATH = f'projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC_ID}'

# --- NEW: Batching Configuration ---
BATCH_SIZE = 5
CUSTOM_TIMEOUT_SECONDS = 120

custom_batch_settings = BatchSettings(max_messages=BATCH_SIZE)

def generate_mock_trade():
    client_info = random.choice(MOCK_CLIENTS)
    ticker = random.choice(TICKERS)
    trade_type = random.choice(TRADE_TYPES)
    price = round(random.uniform(10.00, 2000.00), 2)
    quantity = random.randint(100, 5000)
    currency = random.choice(CURRENCIES)
    exchange = random.choice(EXCHANGES)
    now = datetime.datetime.now()
    timestamp = now.isoformat()
    trade_id = now.strftime("%Y%m%d%H%M%S") + str(now.microsecond) + str(random.randint(100, 999))
    version = random.randint(1, 10)
    maturity_date = (now + datetime.timedelta(days=random.randint(30, 700))).isoformat() + 'Z'

    return {
        'trade_id': trade_id,
        'client_id': client_info['client_id'],
        'client_name': client_info['name'],
        'account_id': client_info['account_id'],
        'version': version,
        'maturity_date': maturity_date,
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

#Generates Trades
def continuous_generation():
    total_trades_sent = 0
    total_pubsub_success = 0
    start_time = time.time()

    futures = []
    try:
        publisher = pubsub_v1.PublisherClient(batch_settings=custom_batch_settings)
        print(f"Connected to PubSub topic: {PUBSUB_TOPIC_PATH} (Batch Size: {BATCH_SIZE})", file=sys.stderr)
    except Exception as e:
        print(f"Failed to initialize PubSub: {e}", file=sys.stderr)
        return

    print("--- Starting High-Frequency Trade Publisher (Batching Enabled) ---", file=sys.stderr)

    try:
        while True:
            trade = generate_mock_trade()
            # Serialize trade to JSON byte string
            data = json.dumps(trade).encode("utf-8")

            # Publish asynchronously and store the future
            future = publisher.publish(PUBSUB_TOPIC_PATH, data=data)
            futures.append(future)

            total_trades_sent += 1
            total_pubsub_success += 1  #increase count

            time.sleep(SLEEP_TIME_SECONDS)

            # Periodic checking
            if total_trades_sent % 100 == 0:
                # Check results of the last 10 futures added, then clear them
                for f in futures[-10:]:
                    try:
                        f.result(timeout=0.01)
                    except TimeoutError:
                        # Future is still processing, ignore
                        pass
                    except Exception as e:
                        print(f"ASYNC BATCH ERROR detected on trade: {e}", file=sys.stderr)
                        # Remove the failed future
                        futures.remove(f)
                futures = [f for f in futures if not f.done()]

                elapsed_time = time.time() - start_time
                tps = total_trades_sent / elapsed_time if elapsed_time > 0 else 0
                print(f"[STAT] Sent: {total_trades_sent:,} | Rate: {tps:,.0f} TPS | Futures pending: {len(futures)}",
                      file=sys.stderr)

    except KeyboardInterrupt:

        print("\n--- Stopping Publisher... ---", file=sys.stderr)
        publisher.stop()

        # Check all remaining futures for final confirmation/errors
        for f in futures:
            try:
                f.result()  # Wait indefinitely for the final batch results
            except Exception as e:
                print(f" FINAL BATCH ERROR: {e}", file=sys.stderr)

        elapsed_time = time.time() - start_time
        tps = total_trades_sent / elapsed_time if elapsed_time > 0 else 0
        print(f"Final Count: {total_trades_sent:,} trades sent. Average Rate: {tps:,.0f} TPS.", file=sys.stderr)


if __name__ == '__main__':
    continuous_generation()