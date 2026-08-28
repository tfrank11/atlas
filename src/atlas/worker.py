import asyncio

import cloudpickle

from atlas.lib.protocol_utils import deserialize_header, read_frame, serialize_request
from atlas.lib.types import (
    ClientSubmitTask,
    RequestType,
    WorkerBusy,
    WorkerFinishedTask,
    WorkerLogin,
)


class Worker:
    scheduler_reconnect_retries = 0
    max_scheduler_reconnect_sec = 60

    async def handle_submit_task(
        self, writer: asyncio.StreamWriter, header: ClientSubmitTask, body: bytes
    ):
        print(f"[Worker] starting task with id={header.task_id}")
        busy_header = WorkerBusy(task_id=header.task_id)
        writer.write(serialize_request(busy_header))
        await writer.drain()

        try:
            task_fn = cloudpickle.loads(body)
        except Exception as e:  # noqa: BLE001
            print(f"[Worker] deserialization error={e}")
            return

        try:
            print("[Worker] starting task")
            print("[Worker] ----------------------------------------")
            task_fn()
            print("[Worker] ----------------------------------------")

            print(f"[Worker] completed task with id={header.task_id}")

            finished_header = WorkerFinishedTask(task_id=header.task_id)
            writer.write(serialize_request(finished_header))
            await writer.drain()

        except Exception as e:  # noqa: BLE001
            print(f"[Worker] task error={e}")
            print("[Worker] ----------------------------------------")

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        print(f"[Worker] peer connected: {peer}")
        while True:
            frame = await read_frame(reader)
            if not frame:
                break
            header = deserialize_header(frame.header_bytes)
            if header.type == RequestType.CLIENT_SUBMIT_TASK:
                await self.handle_submit_task(
                    writer=writer, header=header, body=frame.body_bytes
                )
            if header.type == RequestType.SCHEDULER_ACK_WORKER_LOGIN:
                print("[Worker] received ack from scheduler for successful login")
            else:
                print(f"[Worker] received unexpected header={header}")
        print(f"[Worker] peer disconnected: {peer}")
        writer.close()

    async def start(self, scheduler_host: str, scheduler_port: int):
        try:
            reader, writer = await asyncio.open_connection(
                host=scheduler_host, port=scheduler_port
            )

            worker_login_header = WorkerLogin()
            payload = serialize_request(worker_login_header)
            writer.write(payload)
            print(
                f"[Worker] logging into scheduler at {scheduler_host}:{scheduler_port}"
            )
            await writer.drain()

            await self.handle(writer=writer, reader=reader)
            self.scheduler_reconnect_retries = 0
        except Exception as e:  # noqa: BLE001
            sec_until_retry = min(
                self.max_scheduler_reconnect_sec,
                (2**self.scheduler_reconnect_retries),
            )
            print(f"[Worker] error={e} retrying in {sec_until_retry} seconds")
            await asyncio.sleep(delay=sec_until_retry)
            self.scheduler_reconnect_retries += 1
            await self.start(
                scheduler_host=scheduler_host, scheduler_port=scheduler_port
            )


async def main():
    worker = Worker()
    await worker.start(scheduler_host="localhost", scheduler_port=8700)


asyncio.run(main())
