import pickle
import asyncio
import logging

logger = logging.getLogger(__name__)
_PREFIX_SIZE_MSG_LEN = 16


async def recv_message(stream: asyncio.StreamReader):
    """Async of `recv_message`. Read a prefixed message as known by the server"""
    logger.debug(f"Awaiting reading a new prefixed message size from {stream}")
    sizemsg = await stream.readexactly(_PREFIX_SIZE_MSG_LEN)
    size = int(sizemsg)  # real message length
    logger.debug(f"Awaiting message of size: {size}")
    return await stream.readexactly(size)


async def send(stream: asyncio.StreamWriter, msg):
    """Send a prefixed message as known by the server"""
    msg = _encode_request(msg)
    logger.debug(f"Sending message {msg} of size {len(msg)}")
    # Make a byte field of established length e.g. b'        56'
    size = f"{len(msg):{_PREFIX_SIZE_MSG_LEN}d}".encode()
    stream.write(size)
    await stream.drain()
    stream.write(msg)
    await stream.drain()


async def recv(reader):
    message = await recv_message(reader)
    return _decode_request(message)


# Encoding/decoding of requests and responses
def _decode_request(msg):
    return pickle.loads(msg)


def _encode_request(request):
    return pickle.dumps(request)

