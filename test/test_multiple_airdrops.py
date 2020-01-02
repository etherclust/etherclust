import unittest

from airdrop import findEntities
from networkgenerator.generator import Generator
import pandas as pd


class TestMultipleAirdrops(unittest.TestCase):

    def test_findMultipleEntities(self):
        g = Generator()
        airdropAmount1 = 777
        recipients1 = g.getExistingAddresses(20)
        recipientsAggregated1 = recipients1[:15]

        airdropAmount2 = 1000
        recipients2 = g.getExistingAddresses(100)
        recipientsAggregated2 = recipients2[:30]

        collectors = g.getExistingAddresses(2)

        g.addAirdrop('0xdistributor1', recipients1, airdropAmount1)
        g.addAggregation(recipientsAggregated1, collectors[0], airdropAmount1)

        g.addAirdrop('0xdistributor2', recipients2, airdropAmount2)
        g.addAggregation(recipientsAggregated2, collectors[1], airdropAmount2)

        transfers = g.getGraphEdgeDF()

        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are (15 + 1) + (30 + 1) addresses in the entity table
        self.assertEqual(len(result), 15 + 1 + 30 + 1)
        # they belong to two different entities
        self.assertEqual(result.entity.nunique(), 2)

    def test_findSingleEntityInTwoNetworks(self):

        g1 = Generator(address = "0xFirstNetwork")
        airdropAmount1 = 777
        recipients1 = g1.getExistingAddresses(100)
        # first airdrop has one aggregation to 15 addresses
        recipientsAggregated1 = recipients1[:15]

        g2 = Generator(address = "0xSecondNetwork")
        airdropAmount2 = 1000
        # second airdrop has 50 recipients of the first airdrop
        recipients2 = recipients1[:50]
        # note that the aggregation overlaps with addresses of the first one
        recipientsAggregated2 = recipients1[10:20]

        collectors = g1.getExistingAddresses(2)

        g1.addAirdrop('0xdistributor1', recipients1, airdropAmount1)
        g1.addAggregation(recipientsAggregated1, collectors[0], airdropAmount1)

        g2.addAirdrop('0xdistributor2', recipients2, airdropAmount2)
        g2.addAggregation(recipientsAggregated2, collectors[1], airdropAmount2)

        transfers = g1.getGraphEdgeDF().append(g2.getGraphEdgeDF())

        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are (15 + 1) + (30 + 1) addresses in the entity table
        self.assertEqual(len(result),
          len(set(recipientsAggregated1 + recipientsAggregated2)) + 2)
        # there is just one identified entity!
        self.assertEqual(result.entity.nunique(), 1)


    def test_findSingleEntityGivenIDEX(self):
        # IDEX is not an active EOA, so we shouldn't find it

        g1 = Generator(address = "0xFirstNetwork")
        airdropAmount1 = 777
        recipients1 = g1.getExistingAddresses(100)
        # one aggregation of 20 addresses
        recipientsAggregated1 = recipients1[:20]
        # and another one that isn't a real one
        # instead, its individuals sending their tokens to IDEX (exchange)
        recipientsAggregated2 = recipients1[30:80]

        collector = g1.getExistingAddresses(1)[0]

        IDEX = "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"

        g1.addAirdrop('0xdistributor1', recipients1, airdropAmount1)

        g1.addAggregation(recipientsAggregated1, collector, airdropAmount1)
        g1.addAggregation(recipientsAggregated2, IDEX, airdropAmount1)

        # the collector of the airdrop also sent the airdrop amount to IDEX
        # it could now seem like IDEX collected from many addresses
        g1.addTransfer('0xcollector1', IDEX, airdropAmount1)

        transfers = g1.getGraphEdgeDF()

        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are (15 + 1) addresses in the entity table
        self.assertEqual(len(result),
          len(recipientsAggregated1) + 1)
        # there is just one identified entity!
        self.assertEqual(result.entity.nunique(), 1)
