import asyncio
from dataclasses import dataclass


@dataclass
class ReadFrameRtn:
    header: bytes
    body: bytes
    full_payload: bytes


async def read_frame(reader: asyncio.StreamReader) -> None | ReadFrameRtn:
    try:
        prefix = await reader.readexactly(8)
    except asyncio.IncompleteReadError:
        return None
    header_len = int.from_bytes(bytes=prefix[:4], byteorder="big", signed=False)
    body_len = int.from_bytes(bytes=prefix[4:8], byteorder="big", signed=False)
    header = await reader.readexactly(header_len)
    body = await reader.readexactly(body_len)
    full_payload = prefix + header + body
    return ReadFrameRtn(header=header, body=body, full_payload=full_payload)
