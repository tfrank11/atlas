import asyncio
from dataclasses import dataclass

import msgpack

from atlas.lib.asyncio_utils import every


@dataclass
class WorkerInfo:
    host: str
    port: int
    busy: bool = False


@dataclass
class TaskInfo:
    id: int
    op: str
    data: bytes


class Scheduler:
    tcp_server: asyncio.Server

    workers: list[WorkerInfo]
    task_queue: list[TaskInfo]

    def __init__(self):
        self.task_queue = []
        self.workers = []

    def add_worker(self, worker_info: WorkerInfo):
        self.workers.append(worker_info)

    def recv(self, data: bytes):
        header_len = int.from_bytes(bytes=data[:4], byteorder="big", signed=False)
        header = msgpack.unpackb(packed=data[8 : 8 + header_len])
        print(f"header={header}")

        self.task_queue.append({"id": header["id"], "op": header["op"], "data": data})

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        print(f"connected: {peer}")
        while True:
            data = await reader.read(1024)
            if not data:
                break
            await self.recv(data=data)
            writer.write(b"ack")
            await writer.drain()
        print(f"disconnected: {peer}")
        writer.close()

    def check_task_queue(self):
        print(f"check_task_queue -> see {len(self.task_queue)}")

    async def start(self):
        # start task queue polling interval
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(every(5, self.check_task_queue))

        # start tcp server
        host = "127.0.0.1"
        port = 8786
        print(f"Scheduler: starting tcp server on {host}:{port}")
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 8786)
        async with self.server:
            await self.server.serve_forever()


async def main():
    scheduler = Scheduler()
    worker1 = WorkerInfo(host="127.0.0.1", port=8787)
    scheduler.add_worker(worker1)
    await scheduler.start()


asyncio.run(main())
