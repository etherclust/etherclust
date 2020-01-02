import unittest

from depositeth import getETHDepositTripletChunk
from networkgenerator.generator import Generator
import pandas as pd

import warnings

class TestDepositETHSimple(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ only create the transactions once, but test multiple scenarios
            tx dataframe is stored as a class variable
        """
        cls.tx = pd.DataFrame({"blockNumber":[10, 100, 111],
        "action.from":["0x0", "0xdeposit", "0x1"],
        "action.to":["0xdeposit", "0xexchange", "0xexchange"],
        #"action.value":['100000000000000000000', '99979000000000000000', '100000000000000'],
        "action.value":[100, 99.99, 0.0001],
        "gas":[21000, 21000, 21000],
        "gas_price":[1000000000000,1000000000000,1000000000000]})

        warnings.simplefilter('ignore', category=ImportWarning)


    def test_find_deposit_known_exchange(self):
        exchanges = ["0xexchange"]
        miners = []

        result = getETHDepositTripletChunk(self.tx, exchanges, miners, 100, 0.01)
        # one forward triple has been found
        self.assertEqual(len(result), 1)

    def test_find_no_deposit_unknown_exchange(self):
        exchanges = []
        miners = []

        result = getETHDepositTripletChunk(self.tx, exchanges, miners, 100, 0.01)
        # there are no triplets in the deposits table
        self.assertEqual(len(result), 0)

    def test_find_no_deposit_from_miner(self):
        exchanges = []
        miners = ["0x0"]

        result = getETHDepositTripletChunk(self.tx, exchanges, miners, 100, 0.01)
        # there are no triplets in the deposits table
        self.assertEqual(len(result), 0)
