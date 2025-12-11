import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import io
import json
from datetime import datetime, timedelta
import logging
import os
import sqlite3  # Using SQLite for mock data generation/verification

# Set up logging for Cloud Logging (and local console)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration (Must be updated for GCP deployment) ---
PROJECT_ID = os.environ.get('PROJECT_ID', 'vibrant-mantis-289406')
REGION = 'us-central1'
PUB_SUB_TOPIC = f'projects/{PROJECT_ID}/topics/Trade-Events'
BIGQUERY_APPROVED_TABLE = f'{PROJECT_ID}:trade_analysis.approved_trades_validated'
BIGQUERY_REJECTED_TABLE = f'{PROJECT_ID}:trade_analysis.rejected_trades_audit'

# Define PCollection Tags for Side Outputs
TAG_APPROVED = 'approved_trades'
TAG_REJECTED = 'rejected_trades'

# BigQuery Schema Definitions (Fixed streaming schema error)
APPROVED_SCHEMA = {
    'fields': [
        {'name': 'trade_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'version', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'maturity_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},  # E.g., APPROVED/REPLACED
    ]
}

REJECTED_SCHEMA = {
    'fields': [
        {'name': 'trade_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'version', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'rejection_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_time', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ]
}


# --- 1. State Management and Validation Class (Core Business Logic) ---

class TradeValidator(beam.DoFn):
    """
    Applies business rules and uses Beam State to track the highest version
    received for each trade_id.
    """
    HIGHEST_VERSION_STATE = beam.transforms.userstate.BagStateSpec(
        'highest_version', beam.coders.PickleCoder()
    )

    # FIX: Correctly unpacks the keyed element (trade_id, trade_event_bytes)
    def process(self, keyed_trade, highest_version_state=beam.DoFn.StateParam(HIGHEST_VERSION_STATE)):

        trade_id, trade_event_bytes = keyed_trade

        # 1. Parse the incoming JSON message
        try:
            # FIX: Calling decode on the byte string, not the tuple
            trade = json.loads(trade_event_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON: {trade_event_bytes}")
            return

        new_version = trade.get('version', 0)
        maturity_date_str = trade.get('maturity_date')

        today = datetime.now().strftime('%Y-%m-%d')
        rejection_reason = None

        # --- FIX: Retrieve Current Highest Version from State (Resolves NameError) ---

        existing_versions = list(highest_version_state.read())
        existing_version = max(existing_versions) if existing_versions else 0

        # ------------------ BUSINESS RULES -------------------

        # Rule 1: Reject trades with a lower version than existing.
        if new_version < existing_version:
            rejection_reason = f"Version {new_version} is lower than existing version {existing_version}."

        # Rule 2: Reject trades with a maturity date earlier than today.
        elif maturity_date_str < today:
            rejection_reason = "Maturity date is in the past or invalid."

        # Rule 3: Reject trades with the same version (Require higher version for update).
        elif new_version == existing_version:
            rejection_reason = f"Version {new_version} already exists. Higher version required to update/replace."

        # ------------------ ROUTING -------------------

        if rejection_reason:
            # Route to Rejected sink
            trade['rejection_reason'] = rejection_reason
            trade['ingestion_time'] = datetime.now().isoformat()
            yield beam.pvalue.TaggedOutput(TAG_REJECTED, trade)
        else:
            # Route to Approved sink and update state

            # 1. Update State (only if version is strictly higher)
            if new_version > existing_version:
                # Clear old version(s) and set the new highest version
                highest_version_state.clear()
                highest_version_state.add(new_version)
                logger.info(f"Updated state for ID {trade_id}: New highest version is {new_version}")

            # 2. Add validation status and yield Approved trade
            trade['status'] = 'APPROVED' if existing_version == 0 else 'REPLACED'
            yield beam.pvalue.TaggedOutput(TAG_APPROVED, trade)


# --- 2. Pipeline Definition ---

def run_pipeline(runner='DirectRunner'):
    """Builds and runs the Apache Beam pipeline."""

    # Use DirectRunner for local testing, DataflowRunner for GCP deployment
    options = PipelineOptions(
        runner=runner,
        project=PROJECT_ID,
        region=REGION,
        job_name=f'trade-validation-processor-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        temp_location=f'gs://{PROJECT_ID}-dataflow-temp/temp',
        staging_location=f'gs://{PROJECT_ID}-dataflow-temp/staging',
        streaming=(runner != 'DirectRunner'),  # Only use streaming for DataflowRunner
        save_main_session=True
    )

    # --- MOCK DATA GENERATION FOR LOCAL TESTING ---
    # Create 5 mock trades to test all scenarios
    MOCK_TRADES = [
        # 1. Trade 1: Initial (APPROVED)
        ('T001',
         b'{"trade_id": "T001", "version": 1, "client_id": "C1", "symbol": "MSFT", "price": 100.0, "quantity": 10, "maturity_date": "2030-01-01"}'),
        # 2. Trade 2: Lower Version (REJECTED)
        ('T001',
         b'{"trade_id": "T001", "version": 0, "client_id": "C1", "symbol": "MSFT", "price": 100.0, "quantity": 10, "maturity_date": "2030-01-01"}'),
        # 3. Trade 3: Expired Maturity Date (REJECTED)
        ('T002',
         b'{"trade_id": "T002", "version": 1, "client_id": "C2", "symbol": "GOOG", "price": 200.0, "quantity": 20, "maturity_date": "2020-01-01"}'),
        # 4. Trade 4: Same Version (REJECTED)
        ('T001',
         b'{"trade_id": "T001", "version": 1, "client_id": "C1", "symbol": "MSFT", "price": 110.0, "quantity": 10, "maturity_date": "2030-01-01"}'),
        # 5. Trade 5: Higher Version (REPLACED/APPROVED)
        ('T001',
         b'{"trade_id": "T001", "version": 2, "client_id": "C1", "symbol": "MSFT", "price": 120.0, "quantity": 10, "maturity_date": "2030-01-01"}'),
    ]
    # --- END MOCK DATA ---

    with beam.Pipeline(options=options) as p:

        # 1. Read from In-Memory Mock Data Source
        if runner == 'DirectRunner':
            trades_stream = p | 'CreateMockTrades' >> beam.Create(MOCK_TRADES)
        else:
            # Use Pub/Sub for GCP deployment
            trades_stream = p | 'ReadFromPubSub' >> io.ReadFromPubSub(topic=PUB_SUB_TOPIC)

        # 2. Apply Business Validation (Input is already keyed for DirectRunner mock data)
        if runner == 'DirectRunner':
            keyed_trades = trades_stream
        else:
            # For GCP Pub/Sub, we key by ID first (as done before)
            keyed_trades = trades_stream | 'KeyByTradeID' >> beam.Map(
                lambda trade: (json.loads(trade.decode('utf-8')).get('trade_id'), trade)
            )

        # 3. Apply Validation and Route Outputs
        validated_trades = keyed_trades | 'ValidateAndRoute' >> beam.ParDo(TradeValidator()).with_outputs(
            TAG_APPROVED, TAG_REJECTED
        )

        # Separate the outputs
        approved_trades = validated_trades[TAG_APPROVED]
        rejected_trades = validated_trades[TAG_REJECTED]

        # 4. Write Approved Trades (Locally, we just print for verification)
        approved_trades | 'LogApproved' >> beam.Map(logger.info, 'APPROVED TRADE:')

        # 5. Write Rejected Trades (Locally, we just print for verification)
        rejected_trades | 'LogRejected' >> beam.Map(logger.warning, 'REJECTED TRADE:')

    logger.info("Pipeline completed. Check console output for APPROVED/REJECTED logs.")


if __name__ == '__main__':
    # --- RUN LOCAL VERIFICATION ---
    logger.info("--- Running Local Verification (DirectRunner) ---")
    run_pipeline(runner='DirectRunner')

    # --- INSTRUCTIONS FOR GCP DEPLOYMENT ---
    logger.info("\n--- To Deploy to GCP Dataflow ---")
    logger.info("1. Ensure PROJECT_ID environment variable and GCS buckets are set.")
    logger.info(f"2. Run the following command (Note: Must provide DirectRunner='DataflowRunner'):")
    logger.info(f"python dataflow_validator.py --runner=DataflowRunner")