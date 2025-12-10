import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.transforms import userstate
from apache_beam.coders import StrUtf8Coder
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import window
from datetime import datetime
import json
import logging
import sys

# Configure basic logging
logging.basicConfig(level=logging.INFO, stream=sys.stderr)

# Define TaggedOutput PCollection names
REJECTED_TAG = 'rejected_trades'
APPROVED_TAG = 'approved_trades'

# --- Configuration ---
# BigQuery Schema (Adjust fields to match your actual trade data)
BIGQUERY_SCHEMA = {
    'fields': [
        {'name': 'trade_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'version', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'maturity_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'client_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


# --- Core Stateful Logic ---
class TradeValidatorDoFn(beam.DoFn):
    """
    Stateful DoFn to manage trade versions and apply business rules.
    """

    # Define a state variable to hold the last successfully processed trade (JSON string)
    LAST_TRADE_STATE = userstate.BagStateSpec(
        'last_trade_state',
        coder=StrUtf8Coder()
    )

    def process(self, keyed_trade, last_trade_state=beam.DoFn.StateParam(LAST_TRADE_STATE)):
        # Input is a KV pair: (trade_id, trade_dict)
        trade_id, incoming_trade = keyed_trade

        last_trade_json = last_trade_state.read()
        last_trade = json.loads(last_trade_json) if last_trade_json else None

        is_valid_version = False
        audit_reason = None

        incoming_version = incoming_trade.get('version', 0)

        # 1. STATEFUL VERSIONING CHECKS
        if last_trade:
            last_version = last_trade.get('version', 0)

            # Rule: Reject trades with a lower version than existing.
            if incoming_version < last_version:
                audit_reason = f"REJECTED: Lower version ({incoming_version}) than existing ({last_version})."

            # Rule: Replace trades with the same version.
            elif incoming_version == last_version:
                audit_reason = f"AUDIT: Replaced trade with same version ({incoming_version})."
                incoming_trade['status'] = 'UPDATED'
                is_valid_version = True

            # Accept trades with a higher version.
            elif incoming_version > last_version:
                audit_reason = f"AUDIT: Replaced trade with higher version ({incoming_version} > {last_version})."
                is_valid_version = True

        else:
            # First time seeing this trade_id, always accept.
            is_valid_version = True
            incoming_trade['status'] = 'NEW'

        # 2. BUSINESS RULE CHECKS (Applied only if version is valid)
        if is_valid_version:
            maturity_date_str = incoming_trade.get('maturity_date')

            if maturity_date_str:
                try:
                    # Convert maturity date, handling Z (UTC) suffix
                    maturity_date = datetime.fromisoformat(maturity_date_str.replace('Z', '+00:00'))
                    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

                    # Rule: Reject trades with a maturity date earlier than today (if new/update).
                    if maturity_date.date() < today.date() and incoming_trade['status'] != 'NEW':
                        audit_reason = f"REJECTED: Maturity date ({maturity_date.date()}) is in the past."
                        is_valid_version = False  # Final rejection

                    elif maturity_date < datetime.now():
                        # Rule: Mark trades as expired if the maturity date has passed.
                        incoming_trade['status'] = 'EXPIRED'

                    elif incoming_trade['status'] == 'NEW':
                        incoming_trade['status'] = 'ACTIVE'

                except ValueError:
                    audit_reason = f"REJECTED: Invalid maturity date format: {maturity_date_str}"
                    is_valid_version = False
            else:
                # If no maturity date, default to active
                incoming_trade['status'] = 'ACTIVE'

        # 3. FINAL ROUTING AND STATE UPDATE
        if is_valid_version:
            # Update state and yield to the approved sink
            last_trade_state.write(json.dumps(incoming_trade))
            yield incoming_trade

        else:
            # Yield rejected trade to the audit side output
            audit_record = {
                'trade_id': trade_id,
                'received_timestamp': datetime.now().isoformat(),
                'status': 'REJECTED',
                'reason': audit_reason,
                'payload': json.dumps(incoming_trade)
            }
            yield TaggedOutput(REJECTED_TAG, audit_record)


# --- Pipeline Definition ---
def run_pipeline(argv=None):
    """Main function to define and run the streaming pipeline."""

    # 1. Define custom pipeline options
    class TradeOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument('--input_subscription', required=True, help='Input PubSub subscription.')
            parser.add_argument('--bq_table_approved', required=True, help='BigQuery table for approved trades.')
            parser.add_argument('--pubsub_topic_rejected', required=True,
                                help='PubSub topic for rejected trade audit logs.')

    options = PipelineOptions(argv)
    trade_options = options.view_as(TradeOptions)
    options.view_as(StandardOptions).streaming = True

    # 2. Build the pipeline
    with beam.Pipeline(options=options) as p:
        # A. READ FROM PUBSUB AND KEY BY TRADE_ID
        trades = (p
                  | 'ReadFromPubSub' >> ReadFromPubSub(subscription=trade_options.input_subscription)
                  | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
                  | 'ParseJson' >> beam.Map(json.loads)
                  # Transform to KV: (trade_id, trade_dict) for stateful processing
                  | 'KeyByTradeId' >> beam.Map(lambda trade: (trade['trade_id'], trade))
                  # Use Global Window for state to persist across the entire streaming job
                  | 'GlobalWindow' >> beam.WindowInto(window.GlobalWindows())
                  )

        # B. APPLY STATEFUL VALIDATION LOGIC
        validation_results = (trades
                              | 'ValidateAndStoreState' >> beam.ParDo(TradeValidatorDoFn()).with_outputs(REJECTED_TAG,
                                                                                                         main=APPROVED_TAG)
                              )

        approved_trades = validation_results[APPROVED_TAG]
        rejected_trades = validation_results[REJECTED_TAG]

        # C. WRITE APPROVED TRADES TO BIGQUERY
        approved_trades | 'WriteToBigQuery' >> WriteToBigQuery(
            table=trade_options.bq_table_approved,
            schema=BIGQUERY_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # D. WRITE REJECTED TRADES TO PUBSUB AUDIT LOG
        (rejected_trades
         | 'SerializeAuditLog' >> beam.Map(json.dumps)
         | 'EncodeAuditLog' >> beam.Map(lambda x: x.encode('utf-8'))
         | 'WriteAuditToPubSub' >> beam.io.WriteToPubSub(topic=trade_options.pubsub_topic_rejected)
         )


if __name__ == '__main__':
    run_pipeline()