import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from argus_beam.transforms.velocity import ComputeVelocity


# ──────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────

def make_enriched_record(card_id='CARD-A1B2C3D4', tx_id='tx-001', amount=150.0):
    """
    Returns a dict simulating what EnrichWithRiskZone outputs.
    ComputeVelocity receives these — already enriched, not raw bytes.
    """
    return {
        'tx_id': tx_id,
        'card_id': card_id,
        'amount': amount,
        'currency': 'CAD',
        'merchant_id': 'MERCH-001',
        'lat': 43.65,
        'lon': -79.38,
        'timestamp': '2026-03-07T12:00:00Z',
        'risk_zone': 'STANDARD',
        'risk_score': 10,
        'ingestion_timestamp': '2026-03-07T12:00:00+00:00',
    }


def run_velocity(keyed_groups: list):
    """
    Runs ComputeVelocity directly on pre-grouped (card_id, [tx, ...]) tuples.

    Why do we pass pre-grouped tuples instead of raw records?
    Because in the real pipeline, GroupByKey runs BEFORE ComputeVelocity.
    In tests, we simulate GroupByKey's output directly — this makes tests
    faster (no window machinery) and more focused (testing only our DoFn logic).
    """
    with TestPipeline() as p:
        result = (
            p
            | beam.Create(keyed_groups)
            | beam.ParDo(ComputeVelocity())
        )
        return result


# ──────────────────────────────────────────────
# Count Correctness Tests
# ──────────────────────────────────────────────

class TestVelocityCount:
    """Proves window_tx_count is calculated correctly."""

    def test_single_transaction_gets_count_one(self):
        """A card with 1 transaction in the window → window_tx_count = 1."""
        tx = make_enriched_record()
        group = ('CARD-A1B2C3D4', [tx])

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: r['window_tx_count']),
                equal_to([1]),
                label='check_count'
            )

    def test_three_transactions_get_count_three(self):
        """A card with 3 transactions in the window → all get window_tx_count = 3."""
        txs = [
            make_enriched_record(tx_id='tx-001'),
            make_enriched_record(tx_id='tx-002'),
            make_enriched_record(tx_id='tx-003'),
        ]
        group = ('CARD-A1B2C3D4', txs)

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: r['window_tx_count']),
                equal_to([3, 3, 3]),   # ALL three records get the same count
                label='check_count'
            )

    def test_high_velocity_six_transactions(self):
        """A card with 6 transactions → window_tx_count = 6 (Gold flags > 5)."""
        txs = [make_enriched_record(tx_id=f'tx-{i:03d}') for i in range(6)]
        group = ('CARD-A1B2C3D4', txs)

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: r['window_tx_count']),
                equal_to([6, 6, 6, 6, 6, 6]),
                label='check_count'
            )


# ──────────────────────────────────────────────
# Multi-Card Isolation Tests
# ──────────────────────────────────────────────

class TestCardIsolation:
    """Proves that counts are independent per card — one card's velocity
    does not bleed into another card's count."""

    def test_two_cards_get_independent_counts(self):
        """
        Card A has 3 transactions, Card B has 1 transaction.
        Card A records → window_tx_count = 3
        Card B records → window_tx_count = 1
        They must not contaminate each other.
        """
        card_a_txs = [
            make_enriched_record(card_id='CARD-AAAAAAAA', tx_id='tx-a1'),
            make_enriched_record(card_id='CARD-AAAAAAAA', tx_id='tx-a2'),
            make_enriched_record(card_id='CARD-AAAAAAAA', tx_id='tx-a3'),
        ]
        card_b_txs = [
            make_enriched_record(card_id='CARD-BBBBBBBB', tx_id='tx-b1'),
        ]

        groups = [
            ('CARD-AAAAAAAA', card_a_txs),
            ('CARD-BBBBBBBB', card_b_txs),
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(groups)
                | beam.ParDo(ComputeVelocity())
            )
            # Extract (card_id, window_tx_count) pairs for easy assertion
            assert_that(
                result | beam.Map(lambda r: (r['card_id'], r['window_tx_count'])),
                equal_to([
                    ('CARD-AAAAAAAA', 3),
                    ('CARD-AAAAAAAA', 3),
                    ('CARD-AAAAAAAA', 3),
                    ('CARD-BBBBBBBB', 1),
                ]),
                label='check_isolation'
            )


# ──────────────────────────────────────────────
# Output Shape Tests
# ──────────────────────────────────────────────

class TestOutputShape:
    """Proves the output records have the right shape."""

    def test_window_tx_count_field_added(self):
        """Output records must contain window_tx_count."""
        group = ('CARD-A1B2C3D4', [make_enriched_record()])

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: 'window_tx_count' in r),
                equal_to([True]),
                label='check_field'
            )

    def test_original_fields_preserved(self):
        """
        ComputeVelocity must not drop any fields from the enriched record.
        It only ADDS window_tx_count — everything else must survive.
        """
        group = ('CARD-A1B2C3D4', [make_enriched_record()])
        required_fields = [
            'tx_id', 'card_id', 'amount', 'currency', 'merchant_id',
            'lat', 'lon', 'timestamp', 'risk_zone', 'risk_score',
            'ingestion_timestamp'
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: all(f in r for f in required_fields)),
                equal_to([True]),
                label='check_fields'
            )

    def test_output_record_count_matches_input(self):
        """
        GroupByKey collapses N records into 1 group.
        ComputeVelocity must re-expand them back to N records.
        Input: 1 group of 4 transactions → Output: 4 individual records.
        """
        txs = [make_enriched_record(tx_id=f'tx-{i:03d}') for i in range(4)]
        group = ('CARD-A1B2C3D4', txs)

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([group])
                | beam.ParDo(ComputeVelocity())
            )
            assert_that(
                result | beam.Map(lambda r: r['tx_id']),
                equal_to(['tx-000', 'tx-001', 'tx-002', 'tx-003']),
                label='check_count'
            )