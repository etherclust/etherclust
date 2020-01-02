import argparse
import gc
import os
import config
import sqlite3
import pandas as pd
from tqdm import tqdm
from utils import query_yes_no
from airdrop import findEntities
from depositeth import getETHDepositTriplets
from deposittoken import getTokenDepositTripletChunk

# initiate the parser
parser = argparse.ArgumentParser()
parser.add_argument("-P", "--prepare", help="prepare database tables (this has to be run once!)", action="store_true")
parser.add_argument("-E", "--ethdeposit", help="perform ether deposit address clustering", action="store_true")
parser.add_argument("-T", "--tokendeposit", help="perform token deposit address clustering", action="store_true")
parser.add_argument("-A", "--airdrop", help="perform multi-airdrop participation clustering", action="store_true")
args = parser.parse_args()


def createActiveEOATable():
    print("Finding all active EOAs", end="")
    eoaChunkList = []
    for chunk in pd.read_csv(config.transactionsFile, chunksize=100000):
        chunk = chunk[chunk['traceAddress'] == "[]"]
        eoaChunk = chunk[['action.from']].drop_duplicates()
        eoaChunkList.append(eoaChunk)
        print(".", end = "", flush=True)
    activeEOAs = pd.concat(eoaChunkList).drop_duplicates()
    activeEOAs.columns = ['address']
    activeEOAs.set_index('address', inplace=True)

    print("Storing in database... ", end="")
    cnx = sqlite3.connect(config.dbFile)
    activeEOAs.to_sql("active", cnx, if_exists="replace", index=True)
    cnx.close()
    gc.collect()
    print("Done!")

def createTokenTransferTable():
    print("Converting token transfers to table", end="")
    cnx = sqlite3.connect(config.dbFile)
    for chunk in pd.read_csv(config.tokenTransfersFile, dtype = {'amount':object}, chunksize=100000):
        print(".", end = "", flush=True)
        chunk.set_index('address').to_sql("tokentransfers", cnx, if_exists='append', index=True)
    cnx.close()
    print(" Done!")

def prepare():
    createActiveEOATable()
    createTokenTransferTable()

def main():

    if args.prepare:
        if(os.path.exists(config.dbFile)):
            if(query_yes_no("A database already exists, want to delete it?")):
                os.remove(config.dbFile)
                prepare()
            else:
                print("skipping prepare")
        else:
            prepare()

    if args.ethdeposit:
        triplets = getETHDepositTriplets(config.transactionsFile, config.blocksFile, config.exchangesFile, config.maxBlockDiff, config.maxETHDiff)
        triplets.to_csv("data/output/ethdeposittriplets.csv", index=False)
        print("Done.")

    if args.tokendeposit:
        print("Retrieving all token network addresses")
        cnx = sqlite3.connect(config.dbFile)
        tokenAddresses = pd.read_sql("SELECT DISTINCT address FROM tokentransfers;", cnx).address

        print("Reading exchanges file")
        exchanges = pd.read_csv(config.exchangesFile)
        exchanges = exchanges[(exchanges.accountType == 'eoa') & (exchanges.type == 'Exchange')].address

        result_list = []
        print("Starting analysis for each token network.")
        for address in tqdm(list(tokenAddresses)):
            transfers = pd.read_sql("select * from tokentransfers WHERE address = '{}'".format(address), cnx)
            res = getTokenDepositTripletChunk(transfers, exchanges, config.maxBlockDiff)
            result_list.append(res)
        triplets = pd.concat(result_list)
        triplets.to_csv("data/output/tokendeposittriplets.csv", index=False)
        cnx.close()
        print("Done.")

    if args.airdrop:
        cnx = sqlite3.connect(config.dbFile)
        tokenAddresses = pd.read_sql("SELECT address FROM (select address, count(*) AS count from tokentransfers GROUP BY address)a WHERE count >= {}".format(config.minAirDropRecipients), cnx).address
        tokenAddressCount = len(tokenAddresses)

        result_list = []
        for address in tqdm(list(tokenAddresses)):
            transfers = pd.read_sql("select * from tokentransfers WHERE address = '{}'".format(address), cnx)
            res = findEntities(transfers, minRecipients=config.minAirDropRecipients,
                                  maxBlockDiff=config.maxMedianAirdropBlockDiff, minAggregations=config.minAggregations)
            res['network'] = address
            result_list.append(res)
            #print((num+1)/(tokenAddressCount), len(res))
        result = pd.concat(result_list)

        result.to_csv("data/output/airdrop-entities.csv", index=False)
        cnx.close()

if __name__== "__main__":
    main()
