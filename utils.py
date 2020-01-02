import os
import sys
import sqlite3
import config
import pandas as pd

def median_diff(x):
    return x.diff().median()
median_diff.__name__ = 'medianDiff'


def dataframeToSqlite(csvFile, dbFile, tableName, chunksize = 100000, index = None):
    if os.path.exists(dbFile):
        print("Removing existing database...")
        os.remove(dbFile)
    cnx = sqlite3.connect(dbFile)

    print("Writing data to db", end="")
    for chunk in pd.read_csv(csvFile, chunksize=chunksize):
        print(".", end = "", flush=True)
        if(index is not None):
            chunk.set_index(index, inplace=True)
        chunk.to_sql(tableName, cnx, if_exists='append', index=True)
    print(" Done.")
    cnx.close()


def getActiveAddresses(addressSeries):
    cnx = sqlite3.connect(config.dbFile)
    addressDF = pd.DataFrame({"address":addressSeries})
    addressDF.set_index('address').to_sql("tmpAddresses", cnx, if_exists='replace', index=True)
    res = pd.read_sql("SELECT tmpAddresses.address FROM tmpAddresses LEFT JOIN active ON tmpAddresses.address = active.address WHERE active.address IS NOT NULL", cnx)
    cnx.close()
    return(res.address)

# taken from https://stackoverflow.com/a/3041990
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
