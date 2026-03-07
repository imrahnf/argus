import re
import uuid
import json
from datetime import datetime, timezone

import apache_beam as beam

# /beam/transforms/validate.py
class ValidateTransactions(beam.DoFn):
    '''
    Validates incoming transactions in JSON against the Argus schema
    
    Outputs:
        - 'valid' these can proceed to the bronze layer
        - 'dead_letter' tagged and routed to GCS DLQ
    '''

    # define tags
    VALID = 'valid'
    DEAD_LETTER = 'dead_letter'

    # compiled is faster than re.match
    CARD_PATTERN = re.compile(r'^CARD-[A-Z0-9]{8}$') 

    # valid currencies
    ALLOWED_CURRENCIES = {"CAD", "USD", "EUR", "GBP"}

    def process(self, element):
        '''
        Once per message, takes in raw bytes from pub/sub, decodes, validates, and routes to the appropriate output tag
        '''

        try:
            # decode bytes to string
            raw_string = element.decode('utf-8')
            record = json.loads(raw_string)

            # validate tx_id
            assert isinstance(record.get("tx_id"), str), "tx_id is missing or not a string"
            uuid.UUID(record["tx_id"], version=4)  # will raise if not a valid UUID4

            # validate card ID
            assert self.CARD_PATTERN.match(
                record.get("card_id", "")
            ), f"card_id failed reject: {record.get('card_id')}"

            # validate amount
            assert isinstance(record.get("amount"), (int, float)), "amount is missing or not a number"
            assert 0 < record["amount"] < 1000000, "amount is not in range"

            # validate the currency
            assert record.get("currency") in self.ALLOWED_CURRENCIES, f"currency {record.get('currency')} is not allowed"

            # validate lat/long (lat -+90 = north/south, long -+180 = east/west)
            assert isinstance(record.get("latitude"), (int, float)), "latitude is missing or not a number"
            assert isinstance(record.get("longitude"), (int, float)), "longitude is missing or not a number"
            assert -90 <= record["latitude"] <= 90, "latitude is out of range"
            assert -180 <= record["longitude"] <= 180, "longitude is out of range"

            # timestamp validation
            datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))  # will raise if not valid ISO format

            # if we made it here, it's valid. pass these to the valid tag

            yield beam.pvalue.TaggedOutput(self.VALID, record) # we send the parsed JSON record, not the raw string, to the next step
        
        except Exception as e:
            # send any of these to the dead letter queue with the error message for debugging
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER, {
                "original_payload" : element.decode('utf-8', errors='replace'),  # decode with replacement to avoid issues in DLQ
                "error_type" : type(e).__name__,
                "error_message" : str(e),
                "pipeline_timestamp" : datetime.now(timezone.utc).isoformat()
            })