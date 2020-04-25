[![Build Status](https://travis-ci.org/chrizzFTD/graft.svg?branch=master)](https://travis-ci.org/chrizzFTD/graft)
[![Coverage Status](https://coveralls.io/repos/github/chrizzFTD/graft/badge.svg)](https://coveralls.io/github/chrizzFTD/graft)
[![PyPI version](https://badge.fury.io/py/graft.svg)](https://badge.fury.io/py/graft)
[![PyPI](https://img.shields.io/pypi/pyversions/graft.svg)](https://pypi.python.org/pypi/graft)

# graft
*A bit like raft.*

This is an experiment of learning asyncio and [the raft algorithm.](https://raft.github.io/)

```bash
pip install graft
```
### Warning
If you find this, expect a lot of changes, still unsure about its future.

## Raft
### Starting Raft Network Peers
This is following the [Raft Consensus Algorithm](https://raft.github.io/)
For now:
- 5 servers can start in your local host based on [the config file.](graft/config.py)
- After launching each server (on separate shells), they'll start as followers.
```bash
$ python -m graft.server 1
INFO Serving on ('127.0.0.1', 15000)
INFO 1, Roles.FOLLOWER term: 0, size=0
```
```bash
$ python -m graft.server 2
```
```bash
$ python -m graft.server 3
```
- If they don't hear from a leader after a timeout, a nomination will happen.
```bash
WARNING Didnt hear from leader. Calling an election
INFO 1, Roles.CANDIDATE term: 1, size=0
```
- Only one leader will be elected, even if multiple followers became candidates.
    - Everyone else will become follower again.
- Leader will start replicating a log on all followers.
    - Current entries are only timestamps.
```bash
INFO 1, Roles.LEADER term: 1, size=1
INFO 1, Roles.LEADER term: 1, size=2
INFO 1, Roles.LEADER term: 1, size=3
DEBUG Index 1: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 27, 171511))
DEBUG Index 2: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 27, 674029))
DEBUG Index 3: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 28, 176315))
INFO 1, Roles.LEADER term: 1, size=4
```
- If any server dies (you can `ctrl+c`), when brought back to life it will catchup on the log.
    - If the leader dies, after a bit followers will call an election and cycle will start again.

### TODO:
Commit and everything else.