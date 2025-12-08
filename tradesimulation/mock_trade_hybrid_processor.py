import random
import datetime
import time
import json
import requests
import sys
# --- NEW: Import KafkaProducer ---
from kafka import KafkaProducer

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

# --- NEW: API & Kafka Configuration ---
API_ENDPOINT = "http://127.0.0.1:5000/api/trades/receive"  # Ensure your receiver Flask app is running here
KAFKA_BROKERS = ['localhost:9092']  # **CHECK YOUR KAFKA BROKER ADDRESS**
KAFKA_TOPIC = 'trades-topic'
API_TIMEOUT = 5  # Set a timeout for the API call


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
    """Generates trades continuously and sends them to API and Kafka."""
    total_trades_sent = 0
    total_api_success = 0
    total_kafka_success = 0
    start_time = time.time()

    # Initialize Kafka Producer
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            # Value serializer converts the Python dict to a JSON byte string
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # A good setting for high-throughput, low-latency applications
            linger_ms=10
        )
        print(f"🎉 Connected to Kafka brokers: {KAFKA_BROKERS}")
    except Exception as e:
        print(f"🚨 Failed to initialize Kafka Producer. Only sending to API: {e}")

    print(f"--- 🚀 Starting Hybrid Trade Sender (API: {API_ENDPOINT} | Kafka Topic: {KAFKA_TOPIC}) ---")

    try:
        while True:
            trade = generate_mock_trade()

            # 1. Send to API Endpoint (HTTP POST)
            try:
                response = requests.post(API_ENDPOINT, json=trade, timeout=API_TIMEOUT)
                if response.status_code == 200:
                    total_api_success += 1
                else:
                    # Log API errors to standard error
                    print("API Error: {response.status_code} for trade {trade['trade_id']}")
            except requests.exceptions.RequestException:
                print("API Connection Error for trade {trade['trade_id']}. Skipping API send.")

            # 2. Send to Kafka Queue
            if producer:
                try:
                    # producer.send() is asynchronous. We don't wait for confirmation.
                    producer.send(KAFKA_TOPIC, value=trade)
                    total_kafka_success += 1
                except Exception as e:
                    print(f"❌ Kafka Send Error for trade {trade['trade_id']}: {e}")

            total_trades_sent += 1
            time.sleep(SLEEP_TIME_SECONDS)

            # Periodically print stats to standard error (so it doesn't pollute the data stream)
            if total_trades_sent % 100 == 0:
                elapsed_time = time.time() - start_time
                tps = total_trades_sent / elapsed_time if elapsed_time > 0 else 0
                print(
                    f"[STAT] Sent: {total_trades_sent:,} (API Success: {total_api_success:,} | Kafka Success: {total_kafka_success:,}) | Rate: {tps:,.0f} TPS")

    except KeyboardInterrupt:
        if producer:
            producer.flush()  # Ensure all buffered Kafka messages are sent before exiting
        elapsed_time = time.time() - start_time
        tps = total_trades_sent / elapsed_time if elapsed_time > 0 else 0
        print(f"\n--- 🛑 Hybrid Sender Simulation Stopped ---")
        print(f"Final Count: {total_trades_sent:,} trades sent. Average Rate: {tps:,.0f} TPS.")


if __name__ == '__main__':
    continuous_generation()