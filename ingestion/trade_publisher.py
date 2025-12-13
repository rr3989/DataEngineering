import time
import json
import random
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import os

# --- Configuration ---
PROJECT_ID = os.environ.get('PROJECT_ID', 'vibrant-mantis-289406')
TOPIC_ID = "Trades"
RATE_SECONDS = 0.01

# Initialize the Pub/Sub client
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
except Exception as e:
    print(f"ERROR: Could not initialize Pub/Sub client {e}")
    publisher = None


def get_client_info():
    """Generates mock client data."""
    client_id = random.choice(['C1001', 'C1002', 'C1003', 'C1004', 'C1005'])
    return {'client_id': client_id, 'name': f'Client {client_id}', 'account_id': f'ACC-{client_id}'}


def generate_trade():
    """
    Generates a mock trade message.
    """
    # Use fixed IDs to ensure the message structure matches the validation needs later
    trade_id_suffix = random.choice(["0001", "0002", "0003", "0004", "0005","0006", "0007", "0008", "0009"])
    timestamp_part = datetime.now().strftime('%Y%m%d%H%M%S%f')
    trade_id = timestamp_part + trade_id_suffix

    version = random.randint(1, 10)

    # Simulate maturity dates
    rand_val = random.random()
    if rand_val < 0.8:
        maturity_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
    else:
        maturity_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')

    client_info = get_client_info()

    trade_data = {
        "trade_id": trade_id,
        "version": version,
        "client_id": client_info['client_id'],
        "symbol": random.choice(['AAPL', 'GOOGL', 'MSFT', 'META', 'BK', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']),
        "price": round(random.uniform(100.0, 2000.0), 2),
        "quantity": random.randint(50, 500),
        "side": random.choice(['BUY','SELL']),
        "maturity_date": maturity_date,
        "timestamp": datetime.now().isoformat(),
    }
    return trade_data

def publish_trade(trade_data):
    """Publish trade message to the Pub/Sub topic."""

    data_str = json.dumps(trade_data)
    data = data_str.encode("utf-8")

    future = publisher.publish(
        topic_path,
        data,
        client_id=trade_data['client_id'],
        symbol=trade_data['symbol']
    )

    return future


# --- Main Execution ---
if __name__ == "__main__":
    if publisher is None:
        exit(1)
    print(f"Trade Publisher running. Sending messages to {topic_path}")
    print(f"Press Ctrl+C to stop.")

    try:
        trade_count = 0
        while True:
            trade = generate_trade()
            future = publish_trade(trade)

            trade_count += 1
            if trade_count % 100 == 0:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Published {trade_count} trades. Last ID: {trade['trade_id']}")
            time.sleep(RATE_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping Trade Publisher...")
    except Exception as e:
        print(f"\nAn error occurred: {e}")