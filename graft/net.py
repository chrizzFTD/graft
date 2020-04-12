import asyncio
import logging
import contextlib
from functools import cached_property

from graft import config, transport


logger = logging.getLogger(__name__)


class Network:
    """Maintains connections over the raft network as well as queues for messages"""
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self._active_connections = {}
        self.inbox = asyncio.Queue()
        self._active_ids = self._active_connections.keys()
        self._all_ids = frozenset(config.SERVERS) - {peer_id}
        # have a queue per peer in the network
        self.outbox = {i: asyncio.Queue() for i in self._all_ids}

    def send(self, dest: int, msg):
        self.outbox[dest].put_nowait(msg)

    async def _send_messages(self):
        while True:
            for peer, queue in self.outbox.items():
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:  # no message here, come back later
                    await asyncio.sleep(.01)
                    continue
                queue.task_done()
                try:
                    reader, writer = self._active_connections[peer]
                except KeyError:
                    logger.debug(f"{peer=} not found. Discarded message: {msg}")
                else:
                    logger.debug(f"Sending to {peer=} {msg=}")
                    await transport.send(writer, msg)

    @cached_property
    def peers(self) -> frozenset:
        return self._all_ids

    async def recv(self) -> bytes:
        return await self.inbox.get()

    async def _on_connetion(self, reader, writer):
        logger.info(f"Handling client: {writer.get_extra_info('peername')}")
        connected_peer = await transport.recv(reader)  # first message is always peer_id
        logger.debug(f"Starting to receive from {connected_peer=}")
        with contextlib.suppress(asyncio.IncompleteReadError):
            while message:= await transport.recv(reader):
                await self.inbox.put(message)

        logger.warning(f"Broken peer {connected_peer}. Closing connection.")
        writer.close()
        await writer.wait_closed()
        ## remove all that was going to be sent
        peer_q = self.outbox[connected_peer]
        logger.warning(f"Clearing queue: {peer_q}")
        while peer_q.qsize():
            peer_q.get_nowait()
            peer_q.task_done()

        logger.warning(f"Queue {peer_q} empty.")
        logger.warning(f"Resurrecting connection for {connected_peer}")
        del self._active_connections[connected_peer]
        awaitable = self._connect(connected_peer)
        asyncio.create_task(awaitable)  # resurrect connection and hope for the best

    async def _connect(self, target_peer):
        """Connect to a target_peer by its target_peer ID"""
        host, port = config.SERVERS[target_peer]
        logger.debug(f"Attempting to connect to: {target_peer=}, {host=}, {port=}")
        timeout = 5
        while True:
            awaitable = asyncio.open_connection(host, port)
            try:
                reader, writer = await asyncio.wait_for(awaitable, timeout)
                break
            except (asyncio.TimeoutError, ConnectionRefusedError):
                await asyncio.sleep(1)
        # register ourselves on source connection
        await transport.send(writer, self.peer_id)
        self._active_connections[target_peer] = (reader, writer)
        logger.debug(f"Connected to: {target_peer=}, {host=}, {port=}")

    async def _connect_to_peers(self):
        """Attempt to connect to any missing peers"""
        while missing_peers := self._all_ids - self._active_ids:
            logger.warning(f"{missing_peers=}")
            await asyncio.gather(*(self._connect(peer) for peer in missing_peers))

    async def start(self):
        host, port = config.SERVERS[self.peer_id]
        server = await asyncio.start_server(self._on_connetion, host=host, port=port)
        addr = server.sockets[0].getsockname()
        logger.info(f'Serving on {addr}')
        async with server:
            await asyncio.gather(
                self._connect_to_peers(),
                self._send_messages(),
                server.serve_forever(),
            )


if __name__ == "__main__":
    import argparse
    logger.setLevel(logging.INFO)
    transport.logger.setLevel(logging.INFO)  # tmp: debug too verbose for this module
    parser = argparse.ArgumentParser(description='Start server arguments.')
    parser.add_argument('node', type=int, help='Server node to start')
    parsedargs = parser.parse_args()

    from datetime import datetime

    async def main(peer_id):
        # Have every server send a message to every other server
        async def consume():
            while msg:= await net.recv():
                logger.critical(msg)

        async def broadcast():
            while await asyncio.sleep(peer_id, result=True):
                for peer in net.peers:
                    net.send(peer, f"Hello from: {peer_id}, {datetime.now()}")

        net = Network(peer_id)
        await asyncio.gather(
            net.start(),
            broadcast(),
            consume(),
        )

    asyncio.run(main(parsedargs.node))
