import networkx as nx
import os,binascii
import sqlite3
import config

class Generator(object):
    """docstring for """
    def __init__(self, address = "0xMyToken", addressLength = 20):
        self.g = nx.DiGraph()
        self.address = address
        self.addressLength = addressLength
        self.highestBlock = 0
        self.cnx = sqlite3.connect(config.dbFile)

    def _incBlock(self, num = 1):
        self.highestBlock += num
        return(self.highestBlock)

    def _generateAddress(self):
        return("0x"+binascii.b2a_hex(
        os.urandom(self.addressLength)).decode("utf-8"))

    def generateNewAddresses(self, count = 1):
        addresses = []
        for i in range(count):
            addr = self._generateAddress()
            if(addr not in self.g.nodes() and addr not in addresses):
                addresses += [addr]
        return(addresses)

    def _getExistingAddress(self):
        return(self.cnx.execute("SELECT * FROM active ORDER BY RANDOM() LIMIT 1;").fetchone()[0])

    def getExistingAddresses(self, count = 1):
        rows = self.cnx.execute("SELECT * FROM active ORDER BY RANDOM() LIMIT {};".format(count)).fetchall()
        addresses = [row[0] for row in rows]
        return(addresses)

    def addAirdrop(self, source, recipients, amount, blockdiff=1):
        for i, recipient in enumerate(recipients):
            self.g.add_edge(source, recipient, blockNumber=self._incBlock(blockdiff), amount=amount, type="airdrop")


    def generateNewAirdrop(self, recipientCount, amount, blockdiff=1):
        recipients = self.generateNewAddresses(recipientCount)
        self.addAirdrop(recipients, amount, blockdiff)

    def addAggregation(self, sources, recipient, amount):
        # check if sources really are airdrop recipients
        edges = nx.get_edge_attributes(self.g, "type")
        airdropRecipients = [e[1] for (e,att) in edges.items() if att == "airdrop"]
        isSubset = (set(sources) & set(airdropRecipients)) == set(sources)
        if not isSubset:
            raise Exception("not all source addresses are airdrop recipients!")

        # add aggregation edges
        for i, source in enumerate(sources):
            self.g.add_edge(source, recipient, blockNumber=self._incBlock(), amount=amount, type="aggregation")

    def addTransfer(self, source, target, amount):
        self.g.add_edge(source, target, blockNumber=self._incBlock(), amount=amount, type="manual")

    def getGraph(self):
        return self.g

    def getGraphEdgeDF(self):
        df = nx.to_pandas_edgelist(self.g)
        df['address'] = self.address
        return(df)
