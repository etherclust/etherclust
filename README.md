# etherclust
This repository implements address clustering heuristics for Ethereum with Python 3.

The following is an illustration of addresses clustered in the [Bionic token](https://etherscan.io/token/0xef51c9377feb29856e61625caf9390bd0b67ea18) network:

![bionic token network](https://i.imgur.com/k47UOrd.jpg "Bionic token network")
The airdrop is at F6, the white circle are recipients that never did anything with their tokens. Surrounding, several entities appear to participate multiple times and aggregate their tokens into a single address. At F14, the HotBit exchange is visible. Deposit addresses belonging to HotBit are visible in E12-F13.

To run, the `config.py` file needs to be adjusted with appropriate data.
Example data of the bionic network is included. To run with more data, you first need to extract blocks, transactions, traces and transfers via https://github.com/blockchain-etl/ethereum-etl

### Usage:
```
➜  python etherclust.py -h
usage: etherclust.py [-h] [-P] [-E] [-T] [-A]

optional arguments:
  -h, --help          show this help message and exit
  -P, --prepare       prepare database tables (this has to be run once!)
  -E, --ethdeposit    perform ether deposit address clustering
  -T, --tokendeposit  perform token deposit address clustering
  -A, --airdrop       perform multi-airdrop participation clustering
```
Prepare the database first and then run the heuristics:
- `python etherclust.py -P`
- `python etherclust.py -ETA`

# Package requirements
The following packages need to be installed alongside Python 3:
- `argparse`
- `gc`
- `os`
- `sys`
- `sqlite3`
- `pandas`
- `tqdm`

# Test
Before performing any tests, the database should be initialized (`-P` option)

Run tests with `python -m unittest`
