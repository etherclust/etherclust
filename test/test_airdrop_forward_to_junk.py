import unittest

from airdrop import findEntities
from networkgenerator.generator import Generator
import pandas as pd


class TestAirdropForwardToJunk(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ only build the graph once, to test the detection multiple times
            transfers dataframe is stored as a class variable
        """
        g = Generator()
        airdropAmount = 7777
        recipients = g.generateNewAddresses(20)
        recipientsAggregated = recipients[:15]

        g.addAirdrop('0xdistributor', recipients, airdropAmount)
        # airdropped tokens are sent to an inaccessible address (not wanted)
        g.addAggregation(recipientsAggregated, '0x0000000000000000000000000000000000000000', airdropAmount)

        cls.transfers = g.getGraphEdgeDF()

    def test_findNoEntity(self):
        result = findEntities(self.transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are no addresses in the entity table
        self.assertEqual(len(result), 0)
