import unittest

from airdrop import findEntities
from networkgenerator.generator import Generator
import pandas as pd


class TestAirdropForwardToExchange(unittest.TestCase):

    def test_noEntityWhenForwardToDeposit(self):
        g = Generator(addressLength=2)
        airdropAmount = 7777
        recipients = g.generateNewAddresses(20)
        depositAdresses = g.generateNewAddresses(15)

        g.addAirdrop('0xdistributor', recipients, airdropAmount)
        for i, depositAdress in enumerate(depositAdresses):
            # airdropped tokens are sent to exchange deposit addresses
            g.addTransfer(recipients[i], depositAdress, airdropAmount)
            # and then forwarded to the exchange itself
            g.addTransfer(depositAdress, "0xchange", airdropAmount)

        transfers = g.getGraphEdgeDF()

        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are no addresses in the entity table
        self.assertEqual(len(result), 0)

    def test_noEntityWhenForwardToExchangeContract(self):
        g = Generator(addressLength=2)
        airdropAmount = 7777
        recipients = g.generateNewAddresses(20)
        recipientsAggregated = recipients[:15]
        IDEX = "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"

        g.addAirdrop('0xdistributor', recipients, airdropAmount)
        g.addAggregation(recipientsAggregated, IDEX, airdropAmount)

        transfers = g.getGraphEdgeDF()

        result = findEntities(transfers, minRecipients=10,
                              maxBlockDiff=2, minAggregations=10)
        # there are no addresses in the entity table
        self.assertEqual(len(result), 0)
