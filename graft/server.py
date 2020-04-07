"""
The goal for this project is to make one server (designated in advance as the "leader")
replicate its logger on all of the other servers.

It will do this by sending messages through the network and processing their replies.
You will be able to append new logger entries onto the leader logger and those entries will
just "magically" appear on all of the followers.

The leader will be able to bring any follower up to date if its logger is missing many entries.
Also, the leader will be able to determine consensus.
"""
import logging
import asyncio
from random import randrange
from types import MappingProxyType
from functools import partial, cached_property

from graft import net, state, transport, model

logger = logging.getLogger(__name__)

_MIN_TIMEOUT = 5
_MAX_TIMEOUT = 10


class RaftServer(state.BaseController):
    MESSAGE_DISPATCHER = MappingProxyType({
        model.ElectionRequest: state.State.on_election_request,
        model.ElectionReply: state.State.on_election_reply,
        model.AppendEntriesRequest: state.State.on_append_entries_request,
        model.AppendEntriesReply: state.State.on_append_entries_reply,
    })

    def __init__(self, peer_id: int):
        super().__init__(peer_id)
        self._net = net.Network(peer_id)
        self._retrying_followers = set()
        self._state = state.State()
        self._machine_events = asyncio.Queue()

    async def start(self):
        """Start the server """
        await asyncio.gather(
            self._net.start(),
            self._start_timer(),
            self._dispatch_messages(),
            self._update_state(),
            self._hearbeat(),
        )

    async def _start_timer(self):
        while timeout:= randrange(_MIN_TIMEOUT*100, _MAX_TIMEOUT*100) / 100:
            await asyncio.sleep(timeout)
            await self._add_event(partial(state.State.handle_timeout, self._state, self))

    async def _dispatch_messages(self):
        while msg:= await self._net.recv():
            try:
                function = self.MESSAGE_DISPATCHER[type(msg)]
            except KeyError:
                logger.warning(f"Don't know how to handle {msg=}")
                continue
            await self._add_event(partial(function, self._state, self, msg))

    async def _add_event(self, func):
        await self._machine_events.put(func)

    @cached_property
    def peers(self):
        return self._net.peers

    def send(self, target_peer: int, message: object):
        super().send(target_peer, message)
        self._net.send(target_peer, message)

    async def _update_state(self):
        while method:= await self._machine_events.get():
            method()

    async def _hearbeat(self):
        while await asyncio.sleep(self.peer_id, result=True):
            await self._add_event(partial(state.State.heartbeat, self._state, self))


if __name__ == '__main__':
    # import uvloop
    # uvloop.install()  # be fast
    import faulthandler
    faulthandler.enable()

    import argparse
    transport.logger.setLevel(logging.INFO)  # tmp: debug too verbose for this module
    net.logger.setLevel(logging.INFO)  # tmp: debug too verbose for this module
    state.logger.setLevel(logging.INFO)  # tmp: debug too verbose for this module
    parser = argparse.ArgumentParser(description='Start server arguments.')
    parser.add_argument('node', type=int, help='Server node to start')
    parsedargs = parser.parse_args()

    from datetime import datetime
    node = parsedargs.node

    async def test(peer_id):
        server = RaftServer(peer_id)
        asyncio.create_task(server.start())
        while await asyncio.sleep(peer_id, result=True):
            size = len(server._state.log)
            if server._state.role == state.Roles.LEADER:
                msg = datetime.now()
                server._state.append(server, msg)
                logger.info(f"Leader log size: {size}")
            else:
                logger.info(f"Follower log size: {size}")

    asyncio.run(test(node))
