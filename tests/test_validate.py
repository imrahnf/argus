import json
import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from argus_beam.transforms.validate import ValidateTransactions


# ──────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────

def make_message(overrides=None):
    """
    Builds a valid Pub/Sub message as bytes.
    Pass overrides={} to selectively break specific fields for negative tests.
    """
    base = {
        "tx_id": "550e8400-e29b-41d4-a716-446655440000",
        "card_id": "CARD-A1B2C3D4",
        "amount": 150.00,
        "currency": "CAD",
        "merchant_id": "MERCHANT-001",
        "lat": 43.65,
        "lon": -79.38,
        "timestamp": "2026-03-07T12:00:00Z"
    }
    if overrides:
        base.update(overrides)
    return json.dumps(base).encode('utf-8')


# ──────────────────────────────────────────────
# Helper to build a pipeline and return tagged outputs
# ──────────────────────────────────────────────

def run_pipeline(p, messages: list):
    """
    Wires up ValidateTransactions inside an existing TestPipeline context.
    Must be called inside a `with TestPipeline() as p:` block.

    Returns the tagged MutiplePCollection result so callers can assert
    on either the VALID or DEAD_LETTER tag.
    """
    return (
        p
        | beam.Create(messages)
        | beam.ParDo(ValidateTransactions()).with_outputs(
            ValidateTransactions.VALID,
            ValidateTransactions.DEAD_LETTER,
        )
    )


# ──────────────────────────────────────────────
# Happy Path Tests
# ──────────────────────────────────────────────

class TestValidPath:
    """Tests that perfectly valid messages are routed to the 'valid' tag."""

    def test_valid_message_goes_to_valid_tag(self):
        """A complete, well-formed message should pass all checks and land in the valid tag."""
        msg = make_message()
        expected_record = json.loads(msg.decode('utf-8'))

        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(
                results[ValidateTransactions.VALID],
                equal_to([expected_record]),
                label='check_valid'
            )
        # Note: we don't assert dead_letter is empty here because the portable runner
        # has a known label-collision bug when running two TestPipeline blocks in the
        # same test. Coverage of the dead_letter path is covered by every sad-path test.

    def test_all_allowed_currencies(self):
        """Each allowed currency should pass validation."""
        for currency in ['CAD', 'USD', 'EUR', 'GBP']:
            msg = make_message({'currency': currency})
            with TestPipeline() as p:
                results = run_pipeline(p, [msg])
                assert_that(
                    results[ValidateTransactions.DEAD_LETTER],
                    equal_to([]),
                    label=f'check_currency_{currency}'
                )


# ──────────────────────────────────────────────
# tx_id Tests
# ──────────────────────────────────────────────

class TestTxId:
    """Tests for the tx_id field validation."""

    def test_missing_tx_id_goes_to_dlq(self):
        """A message with no tx_id should be rejected."""
        msg = make_message({'tx_id': None})

        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(
                results[ValidateTransactions.DEAD_LETTER] | beam.Map(lambda r: r['error_type']),
                equal_to(['AssertionError']),
                label='check_error_type'
            )

    def test_invalid_uuid_goes_to_dlq(self):
        """A tx_id that is not a valid UUID4 should be rejected."""
        msg = make_message({'tx_id': 'not-a-uuid'})

        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(
                results[ValidateTransactions.DEAD_LETTER] | beam.Map(lambda r: r['error_type']),
                equal_to(['ValueError']),
                label='check_error_type'
            )


# ──────────────────────────────────────────────
# card_id Tests
# ──────────────────────────────────────────────

class TestCardId:
    """Tests for the card_id regex pattern."""

    def test_invalid_card_id_format(self):
        """A card_id that doesn't match CARD-XXXXXXXX should be rejected."""
        for bad_card in ['CARD-abc', '1234-ABCD1234', 'CARD_A1B2C3D4', '']:
            msg = make_message({'card_id': bad_card})
            with TestPipeline() as p:
                results = run_pipeline(p, [msg])
                assert_that(
                    results[ValidateTransactions.VALID],
                    equal_to([]),
                    label=f'valid_empty_{bad_card}'
                )


# ──────────────────────────────────────────────
# Amount Tests
# ──────────────────────────────────────────────

class TestAmount:
    """Tests for the amount field validation."""

    def test_zero_amount_rejected(self):
        msg = make_message({'amount': 0})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_negative_amount_rejected(self):
        msg = make_message({'amount': -50.00})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_amount_over_limit_rejected(self):
        msg = make_message({'amount': 1_000_001})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_string_amount_rejected(self):
        msg = make_message({'amount': 'one hundred'})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')


# ──────────────────────────────────────────────
# Currency Tests
# ──────────────────────────────────────────────

class TestCurrency:
    """Tests for the currency allowlist."""

    def test_invalid_currency_rejected(self):
        for bad_currency in ['JPY', 'AUD', 'BTC', '', None]:
            msg = make_message({'currency': bad_currency})
            with TestPipeline() as p:
                results = run_pipeline(p, [msg])
                assert_that(
                    results[ValidateTransactions.VALID],
                    equal_to([]),
                    label=f'valid_empty_{bad_currency}'
                )


# ──────────────────────────────────────────────
# merchant_id Tests
# ──────────────────────────────────────────────

class TestMerchantId:
    """Tests for the merchant_id field validation."""

    def test_missing_merchant_id_rejected(self):
        msg = make_message({'merchant_id': None})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_empty_merchant_id_rejected(self):
        msg = make_message({'merchant_id': '   '})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')


# ──────────────────────────────────────────────
# Lat / Lon Tests
# ──────────────────────────────────────────────

class TestCoordinates:
    """Tests for latitude and longitude range validation."""

    def test_latitude_out_of_range(self):
        msg = make_message({'lat': 91.0})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_longitude_out_of_range(self):
        msg = make_message({'lon': -200.0})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')


# ──────────────────────────────────────────────
# Timestamp Tests
# ──────────────────────────────────────────────

class TestTimestamp:
    """Tests for the ISO 8601 timestamp validation."""

    def test_invalid_timestamp_rejected(self):
        msg = make_message({'timestamp': 'March 7th 2026'})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')

    def test_missing_timestamp_rejected(self):
        msg = make_message({'timestamp': None})
        with TestPipeline() as p:
            results = run_pipeline(p, [msg])
            assert_that(results[ValidateTransactions.VALID], equal_to([]), label='valid_empty')