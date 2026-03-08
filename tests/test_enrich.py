import pytest
from datetime import datetime
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from argus_beam.transforms.enrich import EnrichWithRiskZone

def make_valid_record(overrides=None):
    """
    Returns a pre-validated transaction dict — simulating what
    ValidateTransactions VALID output would look like.

    This is a dict (not bytes) because enrich.py receives dicts,
    not raw Pub/Sub bytes. ValidateTransactions already decoded them.
    """
    base = {
        'tx_id': '550e8400-e29b-41d4-a716-446655440000',
        'card_id': 'CARD-A1B2C3D4',
        'amount': 150.0,
        'currency': 'CAD',
        'merchant_id': 'MERCH-001',
        'lat': 43.65,    # Toronto — should be STANDARD
        'lon': -79.38,
        'timestamp': '2026-03-07T12:00:00Z'
    }
    if overrides:
        base.update(overrides)
    return base


def run_enrich(records: list):
    """
    Runs EnrichWithRiskZone in a TestPipeline and returns the output PCollection.
    Note: enrich has ONE output (no tagged split), so no .with_outputs() needed.
    """
    with TestPipeline() as p:
        result = (
            p
            | beam.Create(records)
            | beam.ParDo(EnrichWithRiskZone())
        )
        return result


# ──────────────────────────────────────────────
# Zone Assignment Tests
# ──────────────────────────────────────────────

class TestRiskZoneAssignment:
    """Proves that coordinates are correctly matched to their risk zones."""

    def test_standard_zone_toronto(self):
        """Toronto coordinates should fall in no risk zone → STANDARD."""
        record = make_valid_record({'lat': 43.65, 'lon': -79.38})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_zone']),
                equal_to(['STANDARD']),
                label='check_zone'
            )

    def test_high_risk_zone_1_southeast_asia(self):
        """Coordinates inside Southeast Asia box → HIGH_RISK."""
        record = make_valid_record({'lat': 5.0, 'lon': 105.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_zone']),
                equal_to(['HIGH_RISK']),
                label='check_zone'
            )

    def test_high_risk_zone_2_east_africa(self):
        """Coordinates inside East Africa box → HIGH_RISK."""
        record = make_valid_record({'lat': 10.0, 'lon': 40.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_zone']),
                equal_to(['HIGH_RISK']),
                label='check_zone'
            )

    def test_medium_risk_zone_eastern_europe(self):
        """Coordinates inside Eastern Europe box → MEDIUM_RISK."""
        record = make_valid_record({'lat': 40.0, 'lon': 35.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_zone']),
                equal_to(['MEDIUM_RISK']),
                label='check_zone'
            )

    def test_medium_risk_zone_southern_africa(self):
        """Coordinates inside Southern Africa box → MEDIUM_RISK."""
        record = make_valid_record({'lat': -25.0, 'lon': 25.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_zone']),
                equal_to(['MEDIUM_RISK']),
                label='check_zone'
            )


# ──────────────────────────────────────────────
# Risk Score Tests
# ──────────────────────────────────────────────

class TestRiskScore:
    """Proves the composite score formula is calculated correctly."""

    def test_standard_zone_small_amount(self):
        """
        STANDARD base=10, amount=$150 → amount_factor=0
        Expected score: 10 + 0 = 10
        """
        record = make_valid_record({'lat': 43.65, 'lon': -79.38, 'amount': 150.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_score']),
                equal_to([10]),
                label='check_score'
            )

    def test_high_risk_zone_medium_amount(self):
        """
        HIGH_RISK base=80, amount=$600 → amount_factor = int(600/500)*5 = 5
        Expected score: 80 + 5 = 85
        """
        record = make_valid_record({'lat': 5.0, 'lon': 105.0, 'amount': 600.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_score']),
                equal_to([85]),
                label='check_score'
            )

    def test_amount_factor_capped_at_20(self):
        """
        HIGH_RISK base=80, amount=$5000 → amount_factor would be 50 but caps at 20
        Expected score: min(80 + 20, 100) = 100
        """
        record = make_valid_record({'lat': 5.0, 'lon': 105.0, 'amount': 5000.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_score']),
                equal_to([100]),
                label='check_score'
            )

    def test_score_never_exceeds_100(self):
        """Score must be capped at 100 regardless of inputs."""
        record = make_valid_record({'lat': 5.0, 'lon': 105.0, 'amount': 999_999.0})
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: r['risk_score'] <= 100),
                equal_to([True]),
                label='check_cap'
            )


# ──────────────────────────────────────────────
# Output Shape Tests
# ──────────────────────────────────────────────

class TestOutputShape:
    """Proves the enriched record has all required fields."""

    def test_risk_zone_field_present(self):
        """Output record must contain risk_zone."""
        record = make_valid_record()
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: 'risk_zone' in r),
                equal_to([True]),
                label='check_field'
            )

    def test_risk_score_field_present(self):
        """Output record must contain risk_score."""
        record = make_valid_record()
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: 'risk_score' in r),
                equal_to([True]),
                label='check_field'
            )

    def test_ingestion_timestamp_is_valid_iso(self):
        """ingestion_timestamp must be present and parseable as ISO 8601."""
        record = make_valid_record()
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())

            def is_valid_iso(r):
                try:
                    datetime.fromisoformat(r['ingestion_timestamp'])
                    return True
                except Exception:
                    return False

            assert_that(
                result | beam.Map(is_valid_iso),
                equal_to([True]),
                label='check_timestamp'
            )

    def test_original_fields_preserved(self):
        """Enrichment must not drop any of the original validated fields."""
        record = make_valid_record()
        required_fields = ['tx_id', 'card_id', 'amount', 'currency',
                           'merchant_id', 'lat', 'lon', 'timestamp']
        with TestPipeline() as p:
            result = p | beam.Create([record]) | beam.ParDo(EnrichWithRiskZone())
            assert_that(
                result | beam.Map(lambda r: all(f in r for f in required_fields)),
                equal_to([True]),
                label='check_fields'
            )