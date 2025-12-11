import time
import sqlite3
from datetime import datetime, timedelta
import random
import os

# --- Configuration ---
DATABASE_FILE = "trade_validation_split.db"
TOTAL_TRADES_TO_GENERATE = 5000
RATE_SECONDS = 0.001

#def cleanup_database():
    # if os.path.exists(DATABASE_FILE):
        # os.remove(DATABASE_FILE)
        # print(f"--- Cleaned up: '{DATABASE_FILE}'---")

def get_client_info():
    client_id = random.choice(['C1001', 'C1002', 'C1003', 'C1004', 'C1005'])
    return {'client_id': client_id, 'name': f'Client {client_id}', 'account_id': f'ACC-{client_id}'}

# --- Initialize Database  ---
def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    approved_table_sql = """
    CREATE TABLE IF NOT EXISTS approved_trades (
        trade_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        client_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price REAL NOT NULL,
        quantity INTEGER NOT NULL,
        maturity_date TEXT,
        status TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        ingestion_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_id, version)
    );
    """

    rejected_table_sql = """
    CREATE TABLE IF NOT EXISTS rejected_trades (
        trade_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        client_id TEXT NOT NULL,
        rejection_reason TEXT NOT NULL,
        attempted_maturity_date TEXT,
        ingestion_time DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(approved_table_sql)
    cursor.execute(rejected_table_sql)
    conn.commit()
    return conn, cursor


def generate_trade():
    """Generate trades."""

    timestamp_part = datetime.now().strftime('%Y%m%d%H%M%S%f')
    counter_part = str(random.randint(1000, 9999))

    #create trade_id with above combination
    trade_id = timestamp_part + counter_part


    base_trade_ids = [
        datetime.now().strftime('%Y%m%d%H%M%S%f') + '0001',
        datetime.now().strftime('%Y%m%d%H%M%S%f') + '0002',
        datetime.now().strftime('%Y%m%d%H%M%S%f') + '0003',
        datetime.now().strftime('%Y%m%d%H%M%M%S%f') + '0004',
        datetime.now().strftime('%Y%m%d%H%M%M%S%f') + '0005',
    ]

#generate random trades
    trade_id = random.choice(base_trade_ids)

    #version
    if random.random() < 0.3:
        version = random.randint(5, 10)
    else:
        version = random.randint(1, 4)
        version = random.randint(1, 4)

    #maturity date
    rand_val = random.random()
    if rand_val < 0.70:
        maturity_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
    elif rand_val < 0.90:
        maturity_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
    else:
        maturity_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    client_info = get_client_info()

    return {
        "trade_id": trade_id, "version": version, "client_id": client_info['client_id'],
        "symbol": random.choice(['AAPL', 'GOOGL', 'MSFT', 'META', 'BK', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'BABA', 'WMT']),
        "price": round(random.uniform(100.0, 2000.0), 2),
        "quantity": random.randint(50, 500),
        "maturity_date": maturity_date,
        "timestamp": datetime.now().isoformat(),
    }

# --- Business Validation for maturity and version ---

def insert_validated_trade(conn, cursor, new_trade):
    """Applies business rules and inserts into approved_trades or rejected_trades."""

    trade_id = new_trade['trade_id']
    new_version = new_trade['version']
    new_maturity_date_str = new_trade['maturity_date']

    # Check for the highest existing version in the APPROVED table
    cursor.execute(
        "SELECT version FROM approved_trades WHERE trade_id = ? ORDER BY version DESC LIMIT 1",
        (trade_id,)
    )
    existing_version_data = cursor.fetchone()
    existing_version = existing_version_data[0] if existing_version_data else 0

    # Business Rules
    # Reject trades with a lower version than existing.
    if new_version < existing_version:
        log_rejected_trade(cursor, new_trade, conn, "Lower version than existing (Update rejected).")
        return

    # Reject trades with a maturity date earlier than today.
    today = datetime.now().strftime('%Y-%m-%d')
    if new_maturity_date_str < today:
        log_rejected_trade(cursor, new_trade, conn, "Maturity date is in the past or invalid.")
        return

    # Same/Higher Version and Insertion into APPROVED_TRADES
    final_status = "APPROVED"
    if new_version > existing_version:
        final_status = "REPLACED"  # New version supersedes old ones

    # Insert into Approved Trades
    insert_sql = """
    INSERT INTO approved_trades (trade_id, version, client_id, symbol, price, quantity, maturity_date, status, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    values = (
        new_trade['trade_id'], new_trade['version'], new_trade['client_id'], new_trade['symbol'],
        new_trade['price'], new_trade['quantity'], new_trade['maturity_date'],
        final_status, new_trade['timestamp']
    )

    try:
        cursor.execute(insert_sql, values)
        conn.commit()
        log_trade_action(new_trade, "ACCEPTED", f"Status: {final_status}")
    except sqlite3.IntegrityError:
        # new_version == existing_version
        log_rejected_trade(cursor, new_trade, conn, "Version already exists (Duplicate Key/Same Version).")


def log_rejected_trade(cursor, trade, conn, reason):
    """insert rejected trades to the rejected_trades table"""
    insert_sql = """
    INSERT INTO rejected_trades (trade_id, version, client_id, rejection_reason, attempted_maturity_date)
    VALUES (?, ?, ?, ?, ?);
    """
    values = (
        trade['trade_id'], trade['version'], trade['client_id'], reason, trade['maturity_date']
    )

    cursor.execute(insert_sql, values)
    conn.commit()
    log_trade_action(trade, "REJECTED", f"Reason: {reason}")


def log_trade_action(trade, action, message):
    """Log action on trade for audit."""
    print(f"[{action.ljust(9)}] ID: {trade['trade_id'].ljust(6)} | V: {trade['version']} | {message}")


if __name__ == "__main__":

    #cleanup and initialize db
    #cleanup_database()
    conn, cursor = init_db()
    print(f"\n--- Starting {TOTAL_TRADES_TO_GENERATE} trades ---")

    try:
        trade_count = 0
        while trade_count < TOTAL_TRADES_TO_GENERATE:
            new_trade = generate_trade()

            # business validation and insert logic
            insert_validated_trade(conn, cursor, new_trade)

            trade_count += 1
            if trade_count % 500 == 0:
                print(f"--- Processed {trade_count}/{TOTAL_TRADES_TO_GENERATE} trades. ---")

            time.sleep(RATE_SECONDS)

    except Exception as e:
        print(f"\nAn unexpected error occurred at trade count {trade_count}: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print(f"\n--- Database connection closed. Total trade attempts: {trade_count} ---")

            # Final verification
            conn_verify = sqlite3.connect(DATABASE_FILE)
            cursor_verify = conn_verify.cursor()

            approved_count = cursor_verify.execute("SELECT COUNT(*) FROM approved_trades").fetchone()[0]
            rejected_count = cursor_verify.execute("SELECT COUNT(*) FROM rejected_trades").fetchone()[0]

            print(f"--- FINAL SUMMARY ---")
            print(f"Total Attempts: {trade_count}")
            print(f"Approved Records: {approved_count}")
            print(f"Rejected Records: {rejected_count}")
            conn_verify.close()