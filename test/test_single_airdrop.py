import unittest

from airdrop import findEntities
from networkgenerator.generator import Generator
import pandas as pd


class TestSingleAirdrop(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ only build the graph once, to test the detection multiple times
            transfers dataframe is stored as a class variable
        """
        g = Generator()
        airdropAmount = 7777
        recipients = g.getExistingAddresses(20)
        recipientsAggregated = recipients[:15]

        g.addAirdrop('0xdistributor', recipients, airdropAmount)
        g.addAggregation(recipientsAggregated, recipients[19], airdropAmount)

        cls.transfers = g.getGraphEdgeDF()

    def test_findSingleEntity(self):
        result = findEntities(self.transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are 16 (15+1) addresses in the entity table
        self.assertEqual(len(result), 16)
        # they are all the same entity
        self.assertEqual(result.entity.nunique(), 1)

    def test_findNothingWithHighRequirements(self):
        result = findEntities(self.transfers, minRecipients=21,
                              maxBlockDiff=2, minAggregations=10)
        self.assertEqual(len(result), 0)
