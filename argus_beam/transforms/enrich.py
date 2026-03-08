# /argus_beam/transforms/enrich.py

from datetime import datetime, timezone
import apache_beam as beam

class EnrichWithRiskZone(beam.DoFn):
    ''' Enriches valid transactions with a risk zone
    "Based on WHERE this transaction happened, how risky is it?"
    '''

    # Geo fenced risk zones
    RISK_ZONES = [
        {'name': 'HIGH_RISK',   'lat': (1.0,   10.0),  'lon': (100.0, 115.0), 'base_score': 80},
        {'name': 'HIGH_RISK',   'lat': (5.0,   15.0),  'lon': (30.0,  50.0),  'base_score': 75},
        {'name': 'MEDIUM_RISK', 'lat': (35.0,  45.0),  'lon': (25.0,  45.0),  'base_score': 50},
        {'name': 'MEDIUM_RISK', 'lat': (-35.0, -20.0), 'lon': (15.0,  35.0),  'base_score': 45},
    ]

    # Defult zone if none of these match
    DEFAULT_ZONE = {'name': 'STANDARD', 'base_score': 10}

    def process(self, record):
        ''' once per record by Beam
            record -> the validated transaction from ValidateTransactions
        '''
        lat = record.get("lat")
        lon = record.get("lon")

        # First look up the risk zone based on lat/lon
        matched_zone = self.DEFAULT_ZONE

        for zone in self.RISK_ZONES:
            lat_in_range = zone["lat"][0] <= lat <= zone["lat"][1]
            lon_in_range = zone["lon"][0] <= lon <= zone["lon"][1]

            if lat_in_range and lon_in_range:
                matched_zone = zone
                break
        
        # Compute risk score
        # +5 for every $500 in transaction amount, capped at +20
        amount_factor = min(int(record["amount"] // 500) * 5, 20)

        # Final risk score
        risk_score = min(matched_zone["base_score"] + amount_factor, 100)

        ingestion_timestamp = datetime.now(timezone.utc).isoformat()

        # Add new records in place
        record["risk_zone"] = matched_zone["name"]
        record["risk_score"] = risk_score
        record["ingestion_timestamp"] = ingestion_timestamp

        yield record