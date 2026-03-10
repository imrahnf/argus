# argus_beam/pipeline.py

import json
import time
import uuid
import argparse
from datetime import datetime, timezone

# Apache Beam imports
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

# Local imports
from argus_beam.transforms.validate import ValidateTransactions
from argus_beam.transforms.enrich import EnrichWithRiskZone
from argus_beam.transforms.velocity import ComputeVelocity

# -- Row Formatters --
'''
These shape the rrecords into the schema for BigQuery. They are functions, not DoFns because they do simple mapping
'''

def format_bronze_row(message):
    '''
    Formats raw PubSubMessage into brownze talbe row

    Brozne stores originalk payload. This is the audit layer. If Silver changes later, Bronze lets us
    reprocess without replaying from Pub/Sub

    This function returns a dict matching the raw bronze schema
    '''

    return {
        "ingestion_id" : str(uuid.uuid4()), # unique ID for this ingestion event]
        "raw_payload": message.data.decode('utf-8', errors="replace"), # store the raw JSON, not parsed
        "pubsub_message_id": message.attributes.get("message_id", str(uuid.uuid4())), # unique ID for the Pub/Sub message
        "pubsub_publish_time": message.attributes.get("publish_time", 
                              datetime.now(timezone.utc).isoformat()), # timestamp when Pub/Sub message was published
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat() # when we ingested the message
    }

def format_silver_row(record):
    """
    Format fully enriched + velocity computed record into silver table row.
    At this point, record has been through the three DoFns:
        - ValidateTransactions -> schema is clean
        - EnrichTransactions -> risk_zone, risk_score, ingestion_timestamp have been added
        - ComputeVelocity -> window_tx_count has been added

    
    Returns a dict matching the silver schema
    """

    return {
        "tx_id":               record["tx_id"],
        "card_id":             record["card_id"],
        "amount":              record["amount"],
        "currency":            record["currency"],
        "merchant_id":         record["merchant_id"],
        "lat":                 record["lat"],
        "lon":                 record["lon"],

        # rename "timestamp" → "event_timestamp" (BQ column name)
        # "event_timestamp" = when the TRANSACTION happened (producer)
        # "ingestion_timestamp" = when the PIPELINE processed it
        "event_timestamp":     record["timestamp"],
        "risk_zone":           record["risk_zone"],
        "risk_score":          record["risk_score"],
        "window_tx_count":     record["window_tx_count"],
        "ingestion_timestamp": record["ingestion_timestamp"],

        # carried through from Pub/Sub metadata (attached during validation)
        "pubsub_message_id":   record.get("pubsub_message_id", ""),
    }

# -- Pipeline Builder --

def build_pipeline(p, project_id, subscription, dlq_bucket, bq_dataset):
    """
    Wires all transforms together into single beam DAG.

    Args:
        p: beam.Pipeline instance (real/test)
        project_id: GCP project ID string
        subscription: Pub/Sub subscription path
        dlq_bucket: GCS bucket name for dlq
        bq_dataset: BigQuery dataset ID
    """

    # Read from pubsub
    raw_messages = (
        p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=subscription, with_attributes=True)
    )

    # Validate
    validated = (
        raw_messages | "ExtractBytes" >> beam.Map(lambda msg: msg.data)
        | "Validate" >> beam.ParDo(ValidateTransactions()).with_outputs(
            ValidateTransactions.VALID, ValidateTransactions.DEAD_LETTER
            
        )
    )

    # DLQ
    (validated[ValidateTransactions.DEAD_LETTER]
        | "DLQ_Serialize" >> beam.Map(json.dumps)
        | "DLQ_Window" >> beam.WindowInto(window.FixedWindows(300)) # 5 minute windows for batching
        | "DLQ_Write" >> beam.io.WriteToText(
            f"gs://{dlq_bucket}/errors",
            file_name_suffix=".json",
            num_shards=1, # one file per window to keep dlq simple
        )
    )

    # Write raw messages to bronze
    (
        raw_messages
        | "ToBronzeRow" >> beam.Map(format_bronze_row)
        | "WriteBronze" >> beam.io.WriteToBigQuery(
            f"{project_id}:{bq_dataset}.bronze_raw_transactions",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

    # Enrich valid records
    enriched = (
        validated[ValidateTransactions.VALID]
        | "Enrich" >> beam.ParDo(EnrichWithRiskZone())
    )

    # Sliding window velocity detection
    windowed = (
        enriched 
        | "AddCardKey" >> beam.Map(lambda r: (r["card_id"], r)) # key by card_id for windowing
        | "SlidingWindow" >> beam.WindowInto(window.SlidingWindows(size=300, period=60))
        | "GroupByCard" >> beam.GroupByKey()
        | "ComputeVelocity" >> beam.ParDo(ComputeVelocity())
    )

    # Write records to silver
    (
        windowed
        | "ToSilverRow" >> beam.Map(format_silver_row)
        | "WriteSilver" >> beam.io.WriteToBigQuery(
            f"{project_id}:{bq_dataset}.silver_enriched_transactions",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

def run():
    """
    Configure and run pipline
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True, help="Pub/Sub subscription path")
    parser.add_argument("--dlq_bucket",   required=True, help="GCS bucket for dead letters")
    parser.add_argument("--bq_dataset",   default="argus_dataset", help="BigQuery dataset ID")
    known_args, pipeline_args = parser.parse_known_args()

    # Pipeline options — Beam/Dataflow reads --project, --region, --runner, etc. natively
    options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
    )

    # Read project from GoogleCloudOptions so we don't duplicate --project
    # and avoid argparse prefix-matching collision (--project matching --project_id)
    project_id = options.view_as(GoogleCloudOptions).project

    if not project_id:
        raise ValueError("--project must be provided (e.g. --project=argus-489417)")

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            p,
            project_id=project_id,
            subscription=known_args.subscription,
            dlq_bucket=known_args.dlq_bucket,
            bq_dataset=known_args.bq_dataset
        )

if __name__ == "__main__":
    run()