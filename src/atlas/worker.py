import asyncio

import cloudpickle
import msgpack


def unpack_cloudpickle_fn(body_bytes: bytes) -> callable | None:
    try:
        return cloudpickle.loads(body_bytes)
    except ValueError as e:
        print(f"parsing error={e}")
        return None


async def recv(data: bytes):
    header_len = int.from_bytes(bytes=data[:4], byteorder="big", signed=False)
    body_len = int.from_bytes(bytes=data[4:8], byteorder="big", signed=False)

    header = msgpack.unpackb(packed=data[8 : 8 + header_len])
    body_bytes = data[8 + header_len : 8 + header_len + body_len]
    task_fn = unpack_cloudpickle_fn(body_bytes=body_bytes)
    if not task_fn:
        return

    try:
        print(f"header={header}")
        task_fn()
    except Exception as e:  # noqa: BLE001
        print(f"func error={e}")


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    print(f"connected: {peer}")
    while True:
        data = await reader.read(1024)
        if not data:
            break
        await recv(data=data)
        writer.write(b"ack")
        await writer.drain()
    print(f"disconnected: {peer}")
    writer.close()


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", 8786)
    async with server:
        await server.serve_forever()


asyncio.run(main())
