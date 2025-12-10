import matplotlib
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os


matplotlib.use('Agg')
DATABASE_FILE = "trade_validation_split.db"
REPORT_DIR = "reports"

# --- Data Loading Function ---
def load_data():
    """Connect to SQLite and read data into Pandas DataFrames."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        df_approve = pd.read_sql_query("SELECT * FROM approved_trades", conn)
        df_reject = pd.read_sql_query("SELECT * FROM rejected_trades", conn)
        conn.close()
        return df_approve, df_reject
    except Exception as e:
        print(f"Error loading data from SQLite: {e}")
        return pd.DataFrame(), pd.DataFrame()

def create_summary_pie_chart(df_approve, df_reject):
    total_approved = len(df_approve)
    total_rejected = len(df_reject)
    total_attempts = total_approved + total_rejected

    if total_attempts == 0:
        print("No trade data found to generate summary.")
        return

    labels = ['Approved', 'Rejected']
    sizes = [total_approved, total_rejected]
    colors = ['#4CAF50', '#FF5733']  # Green and Red

    plt.figure(figsize=(7, 7))
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90,
            wedgeprops={'edgecolor': 'black', 'linewidth': 1, 'antialiased': True})
    plt.title(f'Trade Validation Summary (Total: {total_attempts})')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    # Save the chart
    plt.savefig(os.path.join(REPORT_DIR, 'summary_pie_chart.png'))
    plt.close()


def create_rejection_reason_bar_chart(df_reject):
    """Creates a bar chart showing the breakdown of rejection reasons."""
    if df_reject.empty:
        print("No rejected trades found")
        return

    reason_counts = df_reject['rejection_reason'].value_counts()

    plt.figure(figsize=(10, 6))
    reason_counts.plot(kind='bar', color='#3498DB')
    plt.title('Rejection Reasons Breakdown')
    plt.xlabel('Rejection Reason')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--')
    plt.tight_layout()

    # Save the chart
    plt.savefig(os.path.join(REPORT_DIR, 'rejection_reasons_bar_chart.png'))
    plt.close()


def create_approved_price_histogram(df_approve):

    if df_approve.empty:
        print("No approved trades found")
        return

    plt.figure(figsize=(10, 6))
    plt.hist(df_approve['price'], bins=20, color='#FFC300', edgecolor='black')
    plt.title('Distribution of Approved Trade Prices')
    plt.xlabel('Trade Price')
    plt.ylabel('Frequency')
    plt.grid(axis='y', linestyle='--')
    plt.tight_layout()

    # Save the chart
    plt.savefig(os.path.join(REPORT_DIR, 'approved_price_histogram.png'))
    plt.close()


# --- Main Execution ---
if __name__ == "__main__":

    # 1. Create report directory if it doesn't exist
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)

    print(f"Loading data from {DATABASE_FILE}...")
    df_approve, df_reject = load_data()

    if df_approve.empty and df_reject.empty:
        print("Database file not found or empty.")
    else:
        print(f"Data loaded. Approved: {len(df_approve)}, Rejected: {len(df_reject)}.")

        print("Generating charts...")
        create_summary_pie_chart(df_approve, df_reject)
        create_rejection_reason_bar_chart(df_reject)
        create_approved_price_histogram(df_approve)

        print(f"\n--- Report Generated Successfully in reports folder ---")