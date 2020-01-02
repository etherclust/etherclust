import os
import pandas as pd
import networkx as nx
import config

import sqlite3

groupCols = ['address','source','amount']
if os.path.exists(config.dbFile):
    cnx = sqlite3.connect(config.dbFile)

def getInactiveAddresses(addressSeries):
    addressDF = pd.DataFrame({"address":addressSeries})
    addressDF.set_index('address').to_sql("tmpAddresses", cnx, if_exists='replace', index=True)
    res = pd.read_sql("SELECT tmpAddresses.address FROM tmpAddresses LEFT JOIN inactive ON tmpAddresses.address = inactive.address WHERE inactive.address IS NOT NULL", cnx)
    return(res.address)

def getActiveAddresses(addressSeries):
    addressDF = pd.DataFrame({"address":addressSeries})
    addressDF.set_index('address').to_sql("tmpAddresses", cnx, if_exists='replace', index=True)
    res = pd.read_sql("SELECT tmpAddresses.address FROM tmpAddresses LEFT JOIN active ON tmpAddresses.address = active.address WHERE active.address IS NOT NULL", cnx)
    return(res.address)

def getContractAddresses(addressSeries):
    addressDF = pd.DataFrame({"address":addressSeries})
    addressDF.set_index('address').to_sql("tmpAddresses", cnx, if_exists='replace', index=True)
    res = pd.read_sql("SELECT tmpAddresses.address FROM tmpAddresses LEFT JOIN contracts ON tmpAddresses.address = contracts.address WHERE contracts.address IS NOT NULL", cnx)
    return(res.address)

def getMultiOutSameAmount(tokenTransfers, minAirDropRecipients):
    group = tokenTransfers.groupby(groupCols).agg(
        {'blockNumber':'count'}).reset_index()
    group = group[group['blockNumber'] >= minAirDropRecipients]
    multiTransfers = group[groupCols].merge(tokenTransfers, how="left")

    return(multiTransfers)

def getAirdropGroups(multiTransfers, maxMedianAirdropBlockDiff):
    def median_diff(x):
        return x.diff().median()
    median_diff.__name__ = 'medianDiff'

    airdropGroups = multiTransfers.sort_values("blockNumber").groupby(groupCols).agg(
        {"blockNumber":["count", median_diff]}).reset_index()
    airdropGroups = airdropGroups[airdropGroups[("blockNumber", "medianDiff")] <= maxMedianAirdropBlockDiff][groupCols]
    airdropGroups.columns = airdropGroups.columns.droplevel(1)

    return(airdropGroups)

def findAirdropSignatures(transfers, minRecipients, maxBlockDiff):
    sameAmountTransfers = getMultiOutSameAmount(transfers, minRecipients)
    signatures = getAirdropGroups(sameAmountTransfers, maxBlockDiff)
    return(signatures)

########

def findEntities(transfers, minRecipients, maxBlockDiff, minAggregations):
    airdropSigs = findAirdropSignatures(transfers, minRecipients, maxBlockDiff)
    result = pd.DataFrame()

    for row in airdropSigs.itertuples():
        address = getattr(row, "address")
        amount = getattr(row, "amount")
        distributor = getattr(row, "source")
        #print(address, amount, distributor)

        # only consider edges with the same amount as in the airdrop
        edges = transfers[(transfers.address == address) &
                          (transfers.amount == amount) &
                          (transfers.amount != 0)]
        if len(edges) == 0: continue

        # maybe we can remove this by filtering the token transfers
        # to only contain those that have active EOA targets?
        # it would be easy to get these from the regular transactions!
        ##inactiveAddresses = getInactiveAddresses(edges.target.unique())
        ##edges = edges[~edges.target.isin(inactiveAddresses)]
        ##contractAddresses = getContractAddresses(edges.target.unique())
        ##edges = edges[~edges.target.isin(contractAddresses)]

        activeAddresses = getActiveAddresses(edges.target.unique())
        edges = edges[edges.target.isin(activeAddresses)]

        if len(edges[edges.source == distributor]) == 0: continue

        g = nx.from_pandas_edgelist(edges, source="source", target="target", edge_attr="blockNumber", create_using=nx.DiGraph)

        ego_g = nx.ego_graph(g, distributor, radius=2, center=False)
        tmp = nx.to_pandas_edgelist(ego_g, source="intermediary")
        if len(tmp) == 0: continue

        tmp = tmp[tmp.target != distributor]
        foo = tmp[tmp.groupby(['target'])['intermediary'].transform('count') >= minAggregations]

        result = result.append(foo)

    # convert to address-entity mapping
    if(result.empty):
        return pd.DataFrame()

    G=nx.from_pandas_edgelist(result, 'intermediary', 'target')
    l=list(nx.connected_components(G))
    L=[dict.fromkeys(y,x) for x, y in enumerate(l)]
    d={k: v for d in L for k, v in d.items()}

    mapping = pd.DataFrame(list(d.items()), columns=['Id', 'entity'])
    return(mapping)
