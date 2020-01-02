import unittest
import warnings

from airdrop import findEntities
import pandas as pd


class TestNoAirdrop(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', category=RuntimeWarning)
        # Otherwise we get an ugly RuntimeWarning for empty dataframes
        # RuntimeWarning: divide by zero encountered in long_scalars

    def test_noEntitiesInEmptyGraph(self):
        transfers = pd.DataFrame(columns=[
            'address',
            'blockNumber',
            'eventName',
            'source',
            'target',
            'amount',
            ])
        result = findEntities(transfers, 1, 1, 1)
        self.assertEqual(len(result), 0)

    def test_noEntitiesInZeroAmountAirdrop(self):
        from networkgenerator.generator import Generator
        g = Generator()
        airdropAmount = 0
        recipients = g.generateNewAddresses(20)
        recipientsAggregated = recipients[:15]

        g.addAirdrop('0xdistributor', recipients, airdropAmount)
        g.addAggregation(recipientsAggregated, "0xcollector", airdropAmount)

        transfers = g.getGraphEdgeDF()
        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        self.assertEqual(len(result), 0)
