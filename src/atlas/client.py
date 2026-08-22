import asyncio

import cloudpickle
import msgpack


def send(writer: asyncio.StreamWriter, op: str, id: int, task_fn: callable):
    header_obj = {"op": op, "id": id}
    header = msgpack.packb(header_obj, use_bin_type=True)
    header_len = len(header).to_bytes(4, "big", signed=False)

    body = cloudpickle.dumps(task_fn)
    body_len = len(body).to_bytes(4, "big", signed=False)

    payload = header_len + body_len + header + body
    writer.write(payload)


async def main():
    scheduler_host = "127.0.0.1"
    scheduler_port = 8700
    reader, writer = await asyncio.open_connection(scheduler_host, scheduler_port)

    def task_fn():
        print("hello from client")

    send(writer, op="submit", id=1, task_fn=task_fn)
    await writer.drain()
    print(await reader.read(1024))

    writer.close()
    await writer.wait_closed()


asyncio.run(main())
