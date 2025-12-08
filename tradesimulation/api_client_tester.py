import requests
import json

API_URL = "http://127.0.0.1:5000/api/trades"


def fetch_trade_data():
    """Fetches data from the mock API."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        # Parse the JSON response
        data = response.json()

        print(f"✅ Successfully fetched trade data. Total trades: **{data.get('count')}**\n")

        # Print the first trade to show the structure
        if data.get('data'):
            first_trade = data['data'][0]
            print(f"--- Example Trade ---")
            print(json.dumps(first_trade, indent=4))
            print("---------------------")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to the API: {e}")
        print("Please make sure 'mock_api_server.py' is running.")


if __name__ == '__main__':
    fetch_trade_data()