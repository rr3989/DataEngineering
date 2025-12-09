import random
import datetime
import time
import json
import sys
from google.cloud import pubsub_v1

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


# --- Configuration (RE-VERIFY THESE!) ---
GCP_PROJECT_ID = 'vibrant-mantis-289406'
PUBSUB_TOPIC_ID = 'trade-topics'
PUBSUB_TOPIC_PATH = f'projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC_ID}'


# ... (Keep the generate_mock_trade function) ...

def synchronous_diagnostic_run():
    """Sends 10 trades one-by-one, blocking for the API response after each one."""

    # Do NOT pass batch_settings here. We want synchronous, non-batched sends.
    publisher = pubsub_v1.PublisherClient()

    print(f"--- 🚀 Starting SYNCHRONOUS DIAGNOSTIC on {PUBSUB_TOPIC_PATH} ---", file=sys.stderr)

    for i in range(1, 11):  # Send exactly 10 trades
        trade = generate_mock_trade()
        data = json.dumps(trade).encode("utf-8")

        try:
            future = publisher.publish(PUBSUB_TOPIC_PATH, data=data)

            # CRITICAL: This line blocks and waits for the API response.
            message_id = future.result(timeout=5)

            print(f"✅ Success Trade {i}: ID {message_id}")

        except Exception as e:
            # THIS TRACEBACK IS KEY! It will contain the exact GCP HTTP error.
            print(f"❌ CRITICAL GCP ERROR on Trade {i}: {e}", file=sys.stderr)
            print(f"Trade data: {trade}", file=sys.stderr)
            return  # Stop immediately on the first error

def generate_mock_trade():
    """Generates a single, highly randomized trade dictionary with client/market context."""

    client_info = random.choice(MOCK_CLIENTS)
    ticker = random.choice(TICKERS)
    trade_type = random.choice(TRADE_TYPES)
    # ... (other random data generation logic remains the same) ...

    now = datetime.datetime.now()
    timestamp = now.isoformat()
    trade_id = now.strftime("%Y%m%d%H%M%S") + str(now.microsecond) + str(random.randint(100, 999))

    # Add version and a future maturity date for Dataflow validation
    version = random.randint(1, 10)
    maturity_date = (now + datetime.timedelta(days=random.randint(30, 700))).isoformat() + 'Z'

    return {
        'trade_id': trade_id,
        'client_id': client_info['client_id'],
        # ... (rest of the trade dictionary) ...
        'version': version,
        'maturity_date': maturity_date,
        # ... (instrument and transaction details) ...
        'timestamp': timestamp
    }


if __name__ == '__main__':
    synchronous_diagnostic_run()