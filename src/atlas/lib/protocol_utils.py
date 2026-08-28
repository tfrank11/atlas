import asyncio
from dataclasses import asdict, dataclass

import cloudpickle
import msgpack

from atlas.lib.types import HEADER_REGISTRY, RequestHeader, RequestType


@dataclass
class ReadFrameRtn:
    header_bytes: bytes
    body_bytes: bytes
    full_payload_bytes: bytes


async def read_frame(reader: asyncio.StreamReader) -> None | ReadFrameRtn:
    try:
        prefix = await reader.readexactly(8)
    except asyncio.IncompleteReadError:
        return None
    header_len = int.from_bytes(bytes=prefix[:4], byteorder="big", signed=False)
    body_len = int.from_bytes(bytes=prefix[4:8], byteorder="big", signed=False)
    header_bytes = await reader.readexactly(header_len)
    body_bytes = await reader.readexactly(body_len)
    full_payload_bytes = prefix + header_bytes + body_bytes
    return ReadFrameRtn(
        header_bytes=header_bytes,
        body_bytes=body_bytes,
        full_payload_bytes=full_payload_bytes,
    )


def deserialize_header(header_bytes: bytes) -> RequestHeader:
    header_obj = msgpack.unpackb(header_bytes)
    request_type = RequestType(header_obj.pop("type"))
    header_cls = HEADER_REGISTRY[request_type]
    header = header_cls(**header_obj)
    return header


def serialize_request(header_obj: RequestHeader, task_fn=None) -> bytes:
    header_dict = asdict(header_obj)
    header_dict["type"] = header_obj.type.value
    header_bytes = msgpack.packb(header_dict, use_bin_type=True)
    header_len = len(header_bytes).to_bytes(4, "big", signed=False)

    body = cloudpickle.dumps(task_fn) if task_fn is not None else b""
    body_len = len(body).to_bytes(4, "big", signed=False)
    payload = header_len + body_len + header_bytes + body
    return payload
