# functions/gold_refresh/main.py
import os
import logging
from datetime import datetime, timezone

import functions_framework
from google.cloud import bigquery

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Merge SQL
'''
This reads from Silver, computes aggregations, and upserts into the Gold table
'''

MERGE_SQL = """
MERGE `{project}.{dataset}.gold_daily_fraud_summary` AS target
USING (
    WITH silver_today AS (
        -- Read only today's Silver partition
        -- DATE(event_timestamp) enables partition pruning
        -- Without this filter, BigQuery scans ALL Silver data ever written
        SELECT
            card_id,
            amount,
            risk_zone,
            risk_score,
            window_tx_count,
            ingestion_timestamp
        FROM `{project}.{dataset}.silver_enriched_transactions`
        WHERE DATE(event_timestamp) = CURRENT_DATE()
    ),

    aggregated AS (
        SELECT
            CURRENT_DATE()                              AS summary_date,
            COUNT(*)                                    AS total_transactions,

            -- A transaction is "flagged" if it's in any elevated risk zone
            COUNTIF(risk_zone IN ('HIGH_RISK', 'MEDIUM_RISK'))
                                                        AS total_flagged,

            -- Fraud rate: what % of today's transactions were elevated risk
            -- SAFE_DIVIDE avoids division-by-zero if Silver is empty
            -- COALESCE guarantees 0 instead of NULL
            COALESCE(ROUND(
                SAFE_DIVIDE(
                    COUNTIF(risk_zone IN ('HIGH_RISK', 'MEDIUM_RISK')),
                    COUNT(*)
                ) * 100,
            2), 0)                                      AS fraud_rate_pct,

            -- Average risk score across all today's transactions
            COALESCE(ROUND(AVG(risk_score), 2), 0)      AS avg_risk_score,

            -- High velocity = distinct cards that appeared > 5 times in a window
            -- This is the key fraud signal from the velocity layer
            COUNT(DISTINCT CASE
                WHEN window_tx_count > 5 THEN card_id
                ELSE NULL
            END)                                        AS high_velocity_cards,

            -- Total amount across ALL transactions (not just flagged)
            COALESCE(ROUND(SUM(amount), 2), 0)          AS total_amount_processed,

            -- Total financial exposure from flagged transactions only
            COALESCE(ROUND(
                SUM(CASE
                    WHEN risk_zone IN ('HIGH_RISK', 'MEDIUM_RISK') THEN amount
                    ELSE 0
                END),
            2), 0)                                      AS total_amount_flagged,

            -- The risk zone with the most flagged transactions today
            -- Subquery finds the most frequent flagged zone by count
            -- Returns NULL when no flagged transactions exist
            (SELECT risk_zone
             FROM silver_today
             WHERE risk_zone IN ('HIGH_RISK', 'MEDIUM_RISK')
             GROUP BY risk_zone
             ORDER BY COUNT(*) DESC
             LIMIT 1)                                   AS top_risk_zone

        FROM silver_today
        -- HAVING ensures 0 rows when Silver is empty, so MERGE does nothing
        -- Without this, aggregation always returns 1 row with NULLs
        HAVING COUNT(*) > 0
    )
    SELECT * FROM aggregated

-- Match on today's date- this is the UPSERT key
) AS source ON target.summary_date = source.summary_date

-- Row for today already exists sp update all metric columns
WHEN MATCHED THEN UPDATE SET
    target.total_transactions   = source.total_transactions,
    target.total_flagged        = source.total_flagged,
    target.fraud_rate_pct       = source.fraud_rate_pct,
    target.avg_risk_score       = source.avg_risk_score,
    target.high_velocity_cards  = source.high_velocity_cards,
    target.total_amount_processed = source.total_amount_processed,
    target.total_amount_flagged = source.total_amount_flagged,
    target.top_risk_zone        = source.top_risk_zone

-- No row for today yet SO insert fresh
WHEN NOT MATCHED THEN INSERT (
    summary_date,
    total_transactions,
    total_flagged,
    fraud_rate_pct,
    avg_risk_score,
    high_velocity_cards,
    total_amount_processed,
    total_amount_flagged,
    top_risk_zone
) VALUES (
    source.summary_date,
    source.total_transactions,
    source.total_flagged,
    source.fraud_rate_pct,
    source.avg_risk_score,
    source.high_velocity_cards,
    source.total_amount_processed,
    source.total_amount_flagged,
    source.top_risk_zone
)
"""

# Clound functrion entry pointy
@functions_framework.http
def gold_refresh(request):
    """HTTP triggered clound function that reads today's silver transactions
    and upserts a sumamry rowi nto the gold table.
    
    Triggered by: Cloud Scheduler every 5 minutes
    
    Returns:
        tuple (response body string, http status code)
        200 = success, 500 = error
        Cloud Scheduler considers any 2xx response a success and will retry on 5xx
    """

    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset = os.environ.get("BQ_DATASET", "argus_dataset")

    if not project_id:
        logger.error("GCP_PROJECT_ID environment variable not set")
        return "Missing GCP_PROJECT_ID", 500
    
    # Build BQ client
    client = bigquery.Client(project=project_id)

    # Format sql with project and dataset
    sql = MERGE_SQL.format(project=project_id, dataset=dataset)
    logger.info(f"Executing Gold Refresh for date: {datetime.now(timezone.utc).date()}")

    try:
        # execute the merge
        query_job = client.query(sql)
        query_job.result()

        # log the outcome
        rows_affected = query_job.num_dml_affected_rows
        logger.info(
            "Gold refresh complete. Rows affected: %d.\nBytes processed: %d", 
            rows_affected, 
            query_job.total_bytes_processed
        )

        return f"Gold refresh complete. Rows affected: {rows_affected}.", 200
    except Exception as e:
        logger.exception("Gold refresh failed: %s", str(e))
        return f"Gold refresh failed: {str(e)}", 500