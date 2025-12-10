import dash
from dash import dcc, html, dash_table
import plotly.express as px
import pandas as pd
import sqlite3
from datetime import datetime

# --- Configuration ---
DATABASE_FILE = "trade_validation_split.db"
APP_TITLE = "Interactive Trade Validation Dashboard"


# --- Data Loading Function ---
def load_data():
    """This method connects to SQLite and reads data into Pandas DataFrames."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)

        # Load Approved Trades
        df_approved = pd.read_sql_query("SELECT * FROM approved_trades", conn)

        # Load Rejected Trades
        df_rejected = pd.read_sql_query(
            "SELECT trade_id, version, client_id, rejection_reason, attempted_maturity_date FROM rejected_trades", conn)

        conn.close()
        return df_approved, df_rejected
    except Exception as e:
        print(f"Error loading data from SQLite: {e}")
        return pd.DataFrame(), pd.DataFrame()


app = dash.Dash(__name__, title=APP_TITLE)

def serve_layout():
    """Generate the main layout of the dashboard."""
    df_approved, df_rejected = load_data()

    # --- Metrics Calculation ---
    total_approved = len(df_approved)
    total_rejected = len(df_rejected)
    total_attempts = total_approved + total_rejected

    approved_percent = round((total_approved / total_attempts) * 100, 2) if total_attempts > 0 else 0

    # Group By Client ID (Trade Count)
    df_client_count = df_approved.groupby('client_id').size().reset_index(name='Trade Count')
    fig_client = px.bar(df_client_count,
                        x='client_id',
                        y='Trade Count',
                        title='Approved Trades Count by Client ID',
                        color='Trade Count',
                        color_continuous_scale=px.colors.sequential.Teal)

    # Group By Ticker (Total Quantity/Volume)
    df_ticker_volume = df_approved.groupby('symbol')['quantity'].sum().reset_index(name='Total Quantity Traded')
    fig_ticker = px.bar(df_ticker_volume,
                        x='symbol',
                        y='Total Quantity Traded',
                        title='Approved Trade Volume by Ticker',
                        color='Total Quantity Traded',
                        color_continuous_scale=px.colors.sequential.Plasma)

    # --- Summary Charts Data ---
    summary_data = {'Status': ['Approved', 'Rejected'], 'Count': [total_approved, total_rejected]}
    df_summary = pd.DataFrame(summary_data)

    fig_pie = px.pie(df_summary,
                     names='Status',
                     values='Count',
                     title=f'Trade Validation Summary (Total Attempts: {total_attempts})',
                     color='Status',
                     color_discrete_map={'Approved': 'green', 'Rejected': 'red'})

    # Rejection Reason Breakdown Chart ---
    if total_rejected > 0:
        df_rejection_counts = df_rejected['rejection_reason'].value_counts().reset_index()
        df_rejection_counts.columns = ['Reason', 'Count']

        fig_bar = px.bar(df_rejection_counts,
                         x='Reason',
                         y='Count',
                         title='Rejection Reasons Breakdown',
                         color='Reason',
                         color_discrete_sequence=['lightcoral'])
        fig_bar.update_xaxes(tickangle=45)
    else:
        fig_bar = px.bar(pd.DataFrame({'Reason': ['N/A'], 'Count': [0]}), x='Reason', y='Count')


    return html.Div(style={'backgroundColor': '#f0f2f5', 'padding': '20px'}, children=[
        html.H1(APP_TITLE, style={'textAlign': 'center', 'color': '#1f3c66', 'marginBottom': '20px'}),

        # Key Metrics Row
        html.Div([
            html.Div(f"Total Trade Attempts: {total_attempts}", className='col-md-4 card-metric',
                     style={'backgroundColor': '#fff', 'padding': '15px', 'borderRadius': '5px',
                            'boxShadow': '2px 2px 5px #ccc'}),
            html.Div(f"Total Approved: {total_approved}", className='col-md-4 card-metric',
                     style={'backgroundColor': '#fff', 'padding': '15px', 'borderRadius': '5px',
                            'boxShadow': '2px 2px 5px #ccc', 'color': 'darkgreen'}),
            html.Div(f"Approval Rate: {approved_percent}%", className='col-md-4 card-metric',
                     style={'backgroundColor': '#fff', 'padding': '15px', 'borderRadius': '5px',
                            'boxShadow': '2px 2px 5px #ccc', 'color': 'darkblue'}),
        ], className='row', style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': '30px'}),

        # Summary Charts
        html.Div([
            dcc.Graph(id='summary-pie-chart', figure=fig_pie,
                      style={'width': '49%', 'display': 'inline-block', 'border': '1px solid #ccc',
                             'backgroundColor': 'white', 'borderRadius': '5px'}),
            dcc.Graph(id='rejection-bar-chart', figure=fig_bar,
                      style={'width': '49%', 'display': 'inline-block', 'border': '1px solid #ccc',
                             'backgroundColor': 'white', 'borderRadius': '5px'}),
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '30px'}),

        # Group By Charts
        html.H2("Approved Trade Analysis", style={'marginTop': '40px', 'color': '#1f3c66'}),
        html.Div([
            dcc.Graph(id='client-count-chart', figure=fig_client,
                      style={'width': '49%', 'display': 'inline-block', 'border': '1px solid #ccc',
                             'backgroundColor': 'white', 'borderRadius': '5px'}),
            dcc.Graph(id='ticker-volume-chart', figure=fig_ticker,
                      style={'width': '49%', 'display': 'inline-block', 'border': '1px solid #ccc',
                             'backgroundColor': 'white', 'borderRadius': '5px'}),
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '30px'}),

        # Approved Trades Table
        html.H2("Approved Trades Data (View, Sort, Filter)", style={'marginTop': '20px', 'color': 'darkgreen'}),
        dash_table.DataTable(
            id='approved-data-table',
            columns=[{"name": i, "id": i, "deletable": False, "selectable": True} for i in df_approved.columns],
            data=df_approved.to_dict('records'),

            # Interactive features
            filter_action="native",
            sort_action="native",
            page_action="native",
            page_current=0,
            page_size=10,

            style_header={'backgroundColor': 'lightgreen', 'fontWeight': 'bold'},
            style_table={'overflowX': 'auto', 'border': '1px solid #ccc'}
        ),

        # Rejected Trades Table
        html.H2("Rejected Trades Audit Log", style={'marginTop': '40px', 'color': '#CC3333'}),
        dash_table.DataTable(
            id='rejected-data-table',
            columns=[{"name": i, "id": i} for i in df_rejected.columns],
            data=df_rejected.to_dict('records'),
            style_header={'backgroundColor': 'lightcoral', 'fontWeight': 'bold'},
            style_table={'overflowX': 'auto', 'maxHeight': '300px', 'overflowY': 'auto', 'border': '1px solid #ccc'}
        )
    ])


app.layout = serve_layout

# --- Run Application ---
if __name__ == '__main__':
    app.run(debug=True)