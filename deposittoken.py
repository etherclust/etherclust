import os
import pandas as pd
#import networkx as nx
import config
from utils import getActiveAddresses


def getTokenDepositTripletChunk(tokenChunk, exchanges, blockDiff):
    columnsNeeded = ["blockNumber", "source", "target", "amount"]

    tokenChunk = tokenChunk[columnsNeeded].sort_values("blockNumber")
    tokenChunk['block'] = tokenChunk['blockNumber']
    tokenChunk['val'] = tokenChunk['amount']

    # Source should not be a miner ## NOT NEEDED for tokens
    # tokenChunk = tokenChunk[~tokenChunk.from_address.isin(miners)]

    toExchange = tokenChunk[tokenChunk.target.isin(exchanges)].copy()

    potentialDeposits = toExchange.source.drop_duplicates()
    toPotentialDeposit = tokenChunk[tokenChunk.target.isin(potentialDeposits)].copy()
    # filter to active sources
    activeAddresses = getActiveAddresses(toPotentialDeposit.source.drop_duplicates())
    toPotentialDeposit = toPotentialDeposit[toPotentialDeposit.source.isin(activeAddresses)]

    #toPotentialDeposit['val'] = toPotentialDeposit['value'].round(1)

    deposit = pd.merge_asof(left=toExchange, right=toPotentialDeposit,
                            left_on="blockNumber", right_on="blockNumber",
                            left_by=["source", "val"], right_by=["target", "val"],
                            tolerance=blockDiff, direction="backward",
                            allow_exact_matches=True, suffixes=["_y","_x"]).dropna()


    #deposit['amountDiff'] = deposit['amount_y'] - deposit['amount_x']
    #deposit = deposit[(deposit.amountDiff <= amountDiff) & (deposit.amountDiff.round(3) >= 0)]
    depositTraces = deposit[['source_x', 'source_y', 'target_y']]
    depositTraces.columns = ['user', 'deposit', 'exchange']

    return(depositTraces.drop_duplicates())
