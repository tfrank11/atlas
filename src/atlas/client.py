import asyncio
from uuid import uuid4

import cloudpickle

from atlas.lib.protocol_utils import deserialize_header, read_frame, serialize_request
from atlas.lib.types import ClientSubmitTask, RequestType


def send_task(writer: asyncio.StreamWriter, task_id: int, task_fn: callable):
    header_obj = ClientSubmitTask(task_id=task_id)
    payload = serialize_request(header_obj=header_obj, body=task_fn)
    writer.write(payload)


async def main():
    scheduler_host = "127.0.0.1"
    scheduler_port = 8700
    reader, writer = await asyncio.open_connection(scheduler_host, scheduler_port)

    def task_fn():
        print("hello from client")
        return 123

    task_id = str(uuid4())
    send_task(writer, task_id=task_id, task_fn=task_fn)
    await writer.drain()

    while True:
        frame = await read_frame(reader)
        if not frame:
            break

        header = deserialize_header(frame.header_bytes)
        if header.type == RequestType.SCHEDULER_ACK_TASK:
            print(f"[Client] received task ack from scheduler ({header.type})")
        elif header.type == RequestType.WORKER_TASK_FINISHED:
            print(f"[Client] scheduler finished task ({header.task_id})")
            try:
                rtn = cloudpickle.loads(frame.body_bytes)
                print(f"[Client] task finished with return value: {rtn}")
                writer.close()
                await writer.wait_closed()
            except Exception as e:  # noqa: BLE001
                print(f"[Client] error handling worker return value: {e}")


asyncio.run(main())
