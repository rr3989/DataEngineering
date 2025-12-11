import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import io
import json
from datetime import datetime
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# --- Configuration ---
PROJECT_ID = 'vibrant-mantis-289406'
REGION = 'us-central1'
PUB_SUB_TOPIC = f'projects/{PROJECT_ID}/topics/Trades'
BIGQUERY_APPROVED_TABLE = f'{PROJECT_ID}:trade_analysis.approved_trades_validated'
BIGQUERY_REJECTED_TABLE = f'{PROJECT_ID}:trade_analysis.rejected_trades_audit'

# Sinks
TAG_APPROVED = 'approved_trades'
TAG_REJECTED = 'rejected_trades'

APPROVED_SCHEMA = {
    'fields': [
        {'name': 'trade_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'version', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'maturity_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'}, # APPROVED or REPLACED
    ]
}


REJECTED_SCHEMA = {
    'fields': [
        {'name': 'trade_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'version', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'maturity_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'rejection_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_time', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ]
}


class TradeValidator(beam.DoFn):
    """
    Applies business rules for version
    """
    HIGHEST_VERSION_STATE = beam.transforms.userstate.BagStateSpec(
        'highest_version', beam.coders.PickleCoder()
    )

    def process(self, keyed_trade, highest_version_state=beam.DoFn.StateParam(HIGHEST_VERSION_STATE)):

        trade_id, trade_event_bytes = keyed_trade

        # Parse JSON message
        try:
            trade = json.loads(trade_event_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            logging.error(f"Failed to parse JSON: {trade_event_bytes}")
            return

        trade_id = trade.get('trade_id')
        new_version = trade.get('version', 0)
        maturity_date_str = trade.get('maturity_date')

        today = datetime.now().strftime('%Y-%m-%d')
        rejection_reason = None

        existing_versions = list(highest_version_state.read())
        existing_version = max(existing_versions) if existing_versions else 0

        # ------------------ BUSINESS RULES -------------------

        # Reject trades with a lower version than existing.
        if new_version < existing_version:
            rejection_reason = f"Version {new_version} is lower than existing version {existing_version}."

        # Reject trades with a maturity date earlier than today.
        elif maturity_date_str < today:
            rejection_reason = "Maturity date is in the past or invalid."

        # Replace trades with the same version.
        elif new_version == existing_version:
            rejection_reason = f"Version {new_version} already exists. Higher version required to update/replace."

        #Send trade to correct sink
        if rejection_reason:
            # Route to Rejected sink
            trade['rejection_reason'] = rejection_reason
            trade['ingestion_time'] = datetime.now().isoformat()
            yield beam.pvalue.TaggedOutput(TAG_REJECTED, trade)
        else:
            # Route to Approved sink

            if new_version > existing_version:
                highest_version_state.clear()
                highest_version_state.add(new_version)
                logging.info(f"Updated state for ID {trade_id}: New highest version is {new_version}")

            trade['status'] = 'APPROVED' if new_version > existing_version else 'REPLACED'
            yield beam.pvalue.TaggedOutput(TAG_APPROVED, trade)

def run_pipeline():
    # Pipeline for Dataflow
    options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        region=REGION,
        job_name=f'trade-validation-processor-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        temp_location=f'gs://{PROJECT_ID}-dataflow-temp/temp',
        staging_location=f'gs://{PROJECT_ID}-dataflow-temp/staging',
        streaming=True,  #Pub/Sub stream
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        # Read from Pub/Sub
        trades_stream = p | 'ReadFromPubSub' >> io.ReadFromPubSub(topic=PUB_SUB_TOPIC)

        keyed_trades = trades_stream | 'KeyByTradeID' >> beam.Map(
            lambda trade: (json.loads(trade.decode('utf-8')).get('trade_id'), trade)
        )

        validated_trades = keyed_trades | 'ValidateAndRoute' >> beam.ParDo(TradeValidator()).with_outputs(
            TAG_APPROVED, TAG_REJECTED
        )

        approved_trades = validated_trades[TAG_APPROVED]
        rejected_trades = validated_trades[TAG_REJECTED]

        # Write Approved Trades to BigQuery
        approved_trades | 'WriteApprovedToBQ' >> io.WriteToBigQuery(
            table=BIGQUERY_APPROVED_TABLE,
            schema=APPROVED_SCHEMA,
            create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=io.BigQueryDisposition.WRITE_APPEND
        )

        # Write Rejected Trades to BigQuery Audit table
        rejected_trades | 'WriteRejectedToBQ' >> io.WriteToBigQuery(
            table=BIGQUERY_REJECTED_TABLE,
            schema=REJECTED_SCHEMA,
            create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=io.BigQueryDisposition.WRITE_APPEND
        )

    logging.info("Dataflow Pipeline launched.")


# --- Run ---
if __name__ == '__main__':
    run_pipeline()