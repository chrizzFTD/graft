This is an experiment of learning asyncio and the raft algorithm.

Current dependency is [`immutables`](https://github.com/MagicStack/immutables)
```bash
pip install immutables
```
### Warning
If you find this, expect a lot of changes, still unsure about its future.

## Raft
### Starting Raft Network Peers
This is following the [Raft Consensus Algorithm](https://raft.github.io/)
For now:
- 5 servers start in your local host based on [the config file.](config.py)
- After launching each server (on separate shells), they'll start as followers.
```bash
$ python graft/server.py 1
2020-04-16 19:32:33 ChristianLT graft.net[115] INFO Serving on ('127.0.0.1', 15000)
2020-04-16 19:32:19 ChristianLT __main__[101] INFO 1, Roles.FOLLOWER term: 0, size=0
```
```bash
$ python graft/server.py 2
```
```bash
$ python graft/server.py 3
```
- If they don't hear from a leader, a nomination will happen.
```bash
2020-04-16 19:32:26 ChristianLT graft.state[101] WARNING Didnt hear from leader. Calling an election
2020-04-16 19:32:26 ChristianLT __main__[101] INFO 1, Roles.CANDIDATE term: 1, size=0
```
- Only one leader will be elected, everyone else will become follower again.
- Leader will start replicating a log on all followers (current entries are only timestamps)
```bash
# peer 1 selected as leader
2020-04-16 19:32:27 ChristianLT __main__[101] INFO 1, Roles.LEADER term: 1, size=1
2020-04-16 19:32:27 ChristianLT __main__[101] INFO 1, Roles.LEADER term: 1, size=2
2020-04-16 19:32:28 ChristianLT __main__[101] INFO 1, Roles.LEADER term: 1, size=3
2020-04-16 19:32:28 ChristianLT __main__[101] DEBUG Index 1: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 27, 171511))
2020-04-16 19:32:28 ChristianLT __main__[101] DEBUG Index 2: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 27, 674029))
2020-04-16 19:32:28 ChristianLT __main__[101] DEBUG Index 3: Entry(term=1, item=datetime.datetime(2020, 4, 16, 19, 32, 28, 176315))
2020-04-16 19:32:28 ChristianLT __main__[101] INFO 1, Roles.LEADER term: 1, size=4
```
- If a follower dies (you can `ctrl+c`), it will be reconnected once brought back to life.
- If leader dies, when spawning back again it will be a follower, cycle will start again. A new leader will be elected and this new follower will catch up on the replicated log.

### TODO:
Everything else.