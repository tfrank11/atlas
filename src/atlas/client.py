import asyncio
from uuid import uuid4

import cloudpickle

from atlas.lib.protocol_utils import deserialize_header, read_frame, serialize_request
from atlas.lib.types import ClientSubmitTask, RequestType


class Client:
    scheduler_host: str
    scheduler_port: int
    debug: bool

    def __init__(self, scheduler_host: str, scheduler_port: int, debug: bool = False):
        self.scheduler_host = scheduler_host
        self.scheduler_port = scheduler_port
        self.debug = debug

    async def run(self, fn: callable):
        reader, writer = await asyncio.open_connection(
            self.scheduler_host, self.scheduler_port
        )
        task_id = str(uuid4())
        header_obj = ClientSubmitTask(task_id=task_id)
        payload = serialize_request(header_obj=header_obj, body=fn)
        writer.write(payload)

        await writer.drain()

        while True:
            frame = await read_frame(reader)
            if not frame:
                break

            header = deserialize_header(frame.header_bytes)
            if header.type == RequestType.SCHEDULER_ACK_TASK:
                if self.debug:
                    print(f"[Client] received task ack from scheduler ({header.type})")
            elif header.type == RequestType.WORKER_TASK_FINISHED:
                if self.debug:
                    print(f"[Client] scheduler finished task ({header.task_id})")
                try:
                    rtn = cloudpickle.loads(frame.body_bytes)
                    if self.debug:
                        print(f"[Client] task finished with return value: {rtn}")
                    writer.close()
                    await writer.wait_closed()
                    return rtn
                except Exception as e:  # noqa: BLE001
                    print(f"[Client] error handling worker return value: {e}")
                    return None


async def main():
    client = Client(scheduler_host="127.0.0.1", scheduler_port=8700)

    def task1():
        print("hello from client")
        return 100

    res1 = await client.run(task1)
    print(f"res1={res1}")

    def task2():
        return res1 * 2

    res2 = await client.run(task2)
    print(f"res2={res2}")


asyncio.run(main())
