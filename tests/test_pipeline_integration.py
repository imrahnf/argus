import json
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

from argus_beam.transforms.validate import ValidateTransactions
from argus_beam.transforms.enrich import EnrichWithRiskZone
from argus_beam.transforms.velocity import ComputeVelocity
from argus_beam.pipeline import format_bronze_row, format_silver_row


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def make_pubsub_message(overrides=None):
    """
    Simulates a PubsubMessage object.
    In real pipeline: ReadFromPubSub produces these.
    In tests: we build them manually.
    """
    class FakePubsubMessage:
        def __init__(self, data, attributes):
            self.data = data
            self.attributes = attributes

    payload = {
        'tx_id': '550e8400-e29b-41d4-a716-446655440000',
        'card_id': 'CARD-A1B2C3D4',
        'amount': 150.0,
        'currency': 'CAD',
        'merchant_id': 'MERCH-001',
        'lat': 43.65,
        'lon': -79.38,
        'timestamp': '2026-03-07T12:00:00Z',
    }
    if overrides:
        payload.update(overrides)

    return FakePubsubMessage(
        data=json.dumps(payload).encode('utf-8'),
        attributes={
            'message_id': 'test-msg-001',
            'publish_time': '2026-03-07T12:00:00Z',
        }
    )


def make_raw_bytes(overrides=None):
    """Returns raw bytes — what ValidateTransactions actually receives."""
    msg = make_pubsub_message(overrides)
    return msg.data


# ──────────────────────────────────────────────
# Row Formatter Tests
# ──────────────────────────────────────────────

class TestFormatBronzeRow:
    """Proves format_bronze_row produces the correct Bronze schema shape."""

    def test_bronze_row_has_required_fields(self):
        msg = make_pubsub_message()
        row = format_bronze_row(msg)
        required = ['ingestion_id', 'raw_payload', 'pubsub_message_id',
                    'pubsub_publish_time', 'ingestion_timestamp']
        assert all(f in row for f in required)

    def test_bronze_raw_payload_is_original_json_string(self):
        """Bronze must store the raw string — not a parsed dict."""
        msg = make_pubsub_message()
        row = format_bronze_row(msg)
        # raw_payload must be a string (not a dict)
        assert isinstance(row['raw_payload'], str)
        # and it must be valid JSON
        parsed = json.loads(row['raw_payload'])
        assert parsed['tx_id'] == '550e8400-e29b-41d4-a716-446655440000'

    def test_bronze_pubsub_message_id_captured(self):
        msg = make_pubsub_message()
        row = format_bronze_row(msg)
        assert row['pubsub_message_id'] == 'test-msg-001'


class TestFormatSilverRow:
    """Proves format_silver_row shapes the enriched record correctly."""

    def make_enriched_record(self):
        return {
            'tx_id': '550e8400-e29b-41d4-a716-446655440000',
            'card_id': 'CARD-A1B2C3D4',
            'amount': 150.0,
            'currency': 'CAD',
            'merchant_id': 'MERCH-001',
            'lat': 43.65,
            'lon': -79.38,
            'timestamp': '2026-03-07T12:00:00Z',       # input field name
            'risk_zone': 'STANDARD',
            'risk_score': 10,
            'window_tx_count': 2,
            'ingestion_timestamp': '2026-03-07T12:00:00+00:00',
            'pubsub_message_id': 'test-msg-001',
        }

    def test_timestamp_renamed_to_event_timestamp(self):
        """Critical: 'timestamp' must become 'event_timestamp' in Silver."""
        row = format_silver_row(self.make_enriched_record())
        assert 'event_timestamp' in row
        assert 'timestamp' not in row   # old key must not leak through
        assert row['event_timestamp'] == '2026-03-07T12:00:00Z'

    def test_silver_row_has_all_required_fields(self):
        row = format_silver_row(self.make_enriched_record())
        required = [
            'tx_id', 'card_id', 'amount', 'currency', 'merchant_id',
            'lat', 'lon', 'event_timestamp', 'risk_zone', 'risk_score',
            'window_tx_count', 'ingestion_timestamp', 'pubsub_message_id'
        ]
        assert all(f in row for f in required)


# ──────────────────────────────────────────────
# Full Chain Integration Tests
# ──────────────────────────────────────────────

class TestFullChain:
    """
    Tests the validate → enrich → velocity chain end-to-end.
    Uses TestPipeline (DirectRunner) — no GCP credentials needed.
    Does NOT test BigQuery/GCS writes (those require real infra).
    """

    def test_valid_record_survives_full_chain(self):
        """
        A well-formed record must pass through all three transforms
        and emerge with all expected fields intact.
        """
        raw = make_raw_bytes()

        with TestPipeline() as p:
            validated = (
                p
                | beam.Create([raw])
                | beam.ParDo(ValidateTransactions()).with_outputs(
                    ValidateTransactions.VALID, ValidateTransactions.DEAD_LETTER
                    
                )
            )
            enriched = (
                validated[ValidateTransactions.VALID]
                | beam.ParDo(EnrichWithRiskZone())
            )
            # Simulate GroupByKey input for ComputeVelocity
            result = (
                enriched
                | beam.Map(lambda r: (r['card_id'], r))
                | beam.GroupByKey()
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: 'window_tx_count' in r),
                equal_to([True]),
                label='check_velocity_field'
            )

    def test_invalid_record_goes_to_dlq_not_silver(self):
        """
        A malformed record must end up in dead_letter and NOT
        appear anywhere in the valid stream.
        """
        bad_raw = json.dumps({'tx_id': 'not-a-uuid'}).encode('utf-8')

        with TestPipeline() as p:
            validated = (
                p
                | beam.Create([bad_raw])
                | beam.ParDo(ValidateTransactions()).with_outputs(
                    ValidateTransactions.VALID, ValidateTransactions.DEAD_LETTER
                    
                )
            )
            assert_that(
                validated[ValidateTransactions.DEAD_LETTER],
                is_not_empty(),
                label='check_dlq_has_record'
            )

        with TestPipeline() as p:
            validated = (
                p
                | beam.Create([bad_raw])
                | beam.ParDo(ValidateTransactions()).with_outputs(
                    ValidateTransactions.VALID, ValidateTransactions.DEAD_LETTER
                    
                )
            )
            assert_that(
                validated[ValidateTransactions.VALID],
                equal_to([]),
                label='check_valid_is_empty'
            )

    def test_silver_row_shape_after_full_chain(self):
        """
        After the full chain + format_silver_row, the output must
        match the exact Silver BigQuery schema.
        """
        raw = make_raw_bytes()

        with TestPipeline() as p:
            validated = (
                p
                | beam.Create([raw])
                | beam.ParDo(ValidateTransactions()).with_outputs(
                    ValidateTransactions.VALID, ValidateTransactions.DEAD_LETTER
                    
                )
            )
            enriched = (
                validated[ValidateTransactions.VALID]
                | beam.ParDo(EnrichWithRiskZone())
            )
            result = (
                enriched
                | beam.Map(lambda r: (r['card_id'], r))
                | beam.GroupByKey()
                | beam.ParDo(ComputeVelocity())
                | beam.Map(format_silver_row)
            )
            required_fields = [
                'tx_id', 'card_id', 'amount', 'currency', 'merchant_id',
                'lat', 'lon', 'event_timestamp', 'risk_zone', 'risk_score',
                'window_tx_count', 'ingestion_timestamp'
            ]
            assert_that(
                result | beam.Map(
                    lambda r: all(f in r for f in required_fields)
                ),
                equal_to([True]),
                label='check_silver_shape'
            )