from flask import Flask, request, jsonify

app = Flask(__name__)

received_trades = []


@app.route('/api/trades/receive', methods=['POST'])
def receive_trade():
    """Endpoint to receive trade JSON"""

    if not request.json:
        return jsonify({"message": "Error: Request must be JSON"}), 400

    trade_data = request.json
    received_trades.append(trade_data)

    print(
        f" Received Trade: {trade_data.get('trade_id')} | Ticker: {trade_data['instrument']['ticker']} | Total Received: {len(received_trades):,}")

    return jsonify({"message": "Trade received successfully", "trade_id": trade_data.get('trade_id')}), 200


if __name__ == '__main__':
    print("---Starting Trade Receiver API on http://127.0.0.1:5000/ ---")
    app.run(port=5000, debug=True, use_reloader=False)