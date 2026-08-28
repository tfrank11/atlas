import asyncio
from dataclasses import asdict
from uuid import uuid4

from atlas.lib.protocol_utils import deserialize_header, read_frame, serialize_request
from atlas.lib.types import ClientSubmitTask


def send_task(writer: asyncio.StreamWriter, task_id: int, task_fn: callable):
    header_obj = ClientSubmitTask(task_id=task_id)
    print("header_obj", asdict(header_obj))
    payload = serialize_request(header_obj=header_obj, task_fn=task_fn)
    writer.write(payload)


async def main():
    scheduler_host = "127.0.0.1"
    scheduler_port = 8700
    reader, writer = await asyncio.open_connection(scheduler_host, scheduler_port)

    def task_fn():
        print("hello from client")

    task_id = str(uuid4())
    send_task(writer, task_id=task_id, task_fn=task_fn)
    await writer.drain()
    frame = await read_frame(reader)
    if frame:
        header = deserialize_header(frame.header_bytes)
        writer.close()
        await writer.wait_closed()


asyncio.run(main())
