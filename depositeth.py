import os
import pandas as pd
#import networkx as nx
import config



def getETHDepositTripletChunk(txChunk, exchanges, miners, blockDiff, valueDiff):
    columnsNeeded = ["blockNumber", "action.from", "action.to", "action.value"]

    # working with very large numbers converted to pandas floats can lead to
    # slight rounding errors, so we have to round at the end of this function
    #txChunk['action.value'] = txChunk['action.value'].apply(lambda x: int(x)/10**18)
    #txChunk['gas_price'] = txChunk['gas_price'].apply(lambda x: int(x)/10**18)
    #txChunk['fee'] = txChunk['gas'] * txChunk['gas_price']
    txChunk = txChunk[columnsNeeded].sort_values("blockNumber")
    txChunk['block'] = txChunk['blockNumber']

    # Source should not be a miner
    txChunk = txChunk[~txChunk['action.from'].isin(miners)]

    toExchange = txChunk[txChunk['action.to'].isin(exchanges)].copy()
    #toExchange['val'] = (toExchange['action.value'] + toExchange['fee']).round(1)
    toExchange['val'] = toExchange['action.value'].round(1)

    potentialDeposits = toExchange['action.from'].drop_duplicates()
    toPotentialDeposit = txChunk[txChunk['action.to'].isin(potentialDeposits)].copy()
    toPotentialDeposit['val'] = toPotentialDeposit['action.value'].round(1)

    deposit = pd.merge_asof(left=toExchange, right=toPotentialDeposit,
                            left_on="block", right_on="block",
                            left_by=["action.from", "val"], right_by=["action.to", "val"],
                            tolerance=blockDiff, direction="backward",
                            allow_exact_matches=True, suffixes=["_y","_x"]).dropna()

    deposit['blockDiff'] = deposit['blockNumber_y'] - deposit['blockNumber_x']
    deposit['valueDiff'] = (deposit['action.value_x'] - deposit['action.value_y'])

    # need to round to avoid very small differences (i.e. 1e-15)
    deposit = deposit[(deposit.valueDiff.round(3) <= valueDiff) & (deposit.valueDiff.round(3) >= 0)]
    depositTraces = deposit[['action.from_x', 'action.from_y', 'action.to_y', 'blockDiff']]
    depositTraces.columns = ['user', 'deposit', 'exchange', 'blockDiff']

    return(depositTraces.drop_duplicates())

def getETHDepositTriplets(transactionsFilePath, blocksFilePath, exchangesFilePath,  maxBlockDiff, maxETHDiff):
    print("Reading exchanges file")
    exchanges = pd.read_csv(exchangesFilePath)
    exchanges = exchanges[(exchanges.accountType == 'eoa') & (exchanges.type == 'Exchange')].address
    print("Reading blocks file to extract miners")
    miners = pd.read_csv(blocksFilePath).miner.drop_duplicates()

    triplet_dfs = []
    previousChunk = pd.DataFrame()
    previousMaxBlock = 0

    print("Processing chunks of transactions.")
    # chunksize assuming maximum of 10000 transactions per block
    # so that we have at least maxBlockDiff blocks of transactions per chunk
    for chunk in pd.read_csv(transactionsFilePath, chunksize=10000*config.maxBlockDiff):
        minBlock = chunk.blockNumber.min()
        combined = previousChunk.append(chunk)
        triplets = getETHDepositTripletChunk(combined, exchanges, miners, maxBlockDiff, maxETHDiff).dropna()
        triplet_dfs.append(triplets)

        if(chunk.blockNumber.max() - chunk.blockNumber.min() <= maxBlockDiff):
            print("Choose a larger chunksize! Not enough blocks covered!")
        previousChunk = chunk[chunk.blockNumber >= (minBlock - (maxBlockDiff +1))].copy()
        print(".", end = "", flush=True)

    return(pd.concat(triplet_dfs).drop_duplicates())
