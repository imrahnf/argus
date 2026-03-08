# argus_beam/transforms/velocity.py
import apache_beam as beam

class ComputeVelocity(beam.DoFn):
    '''
    Count how many treansactions per card made witin the curretn window,
    and attaches that count to every transaction as 'window_tx_count'
    - it outputs the same transaction that was input, but with an additional field for the count (window_tx_count)

    Why this may matter:
    - window_tx_count > 5 in a 5 minute window -> potential fruad/bot activity
    - feeds the gold layer's 'high_velocity_cards' metrics
    - contribues to silkver table for per transaction analysis
    '''
    def process(self, element):
        card_id, transactions = element # transactions is an iterable of all transactions for this card_id in the current window
        
        tx_list = list(transactions)
        tx_count = len(tx_list)

        # attach count to every transaction in the group and yield
        # every tx in the window for this card gets the same count
        for tx in tx_list:
            tx["window_tx_count"] = tx_count
            yield tx