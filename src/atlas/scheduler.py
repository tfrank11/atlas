import asyncio
from collections import deque
from dataclasses import dataclass

import msgpack

from atlas.lib.asyncio_utils import every
from atlas.lib.reader_utils import read_frame


@dataclass
class WorkerInfo:
    host: str
    port: int


@dataclass
class TaskInfo:
    id: int
    op: str
    data: bytes


class Scheduler:
    tcp_server: asyncio.Server

    workers: list[WorkerInfo]
    task_queue: deque[TaskInfo]

    def __init__(self):
        self.task_queue = deque()
        self.workers = []

    def add_worker(self, worker_info: WorkerInfo):
        self.workers.append(worker_info)

    def recv(self, header: bytes, full_payload: bytes):
        header = msgpack.unpackb(packed=header)
        print(f"[Scheduler] recevied task id={header['id']} op={header['op']}")
        task = TaskInfo(id=header["id"], op=header["op"], data=full_payload)
        self.task_queue.append(task)
        self.check_task_queue()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        print(f"[Scheduler] client connected: {peer}")
        while True:
            frame = await read_frame(reader)
            if not frame:
                break
            self.recv(header=frame.header, full_payload=frame.full_payload)
            writer.write(b"ack")
            await writer.drain()
        print(f"[Scheduler] client disconnected: {peer}")
        writer.close()

    async def check_task_queue(self):
        if not self.task_queue:
            return
        print(f"[Scheduler] check_task_queue -> see {len(self.task_queue)} tasks")
        for worker in self.workers:
            if not self.task_queue:
                break
            task = self.task_queue.popleft()
            await self.dispatch_task(task=task, worker=worker)

    async def dispatch_task(self, worker: WorkerInfo, task: TaskInfo):
        reader, writer = await asyncio.open_connection(
            host=worker.host, port=worker.port
        )
        writer.write(data=task.data)
        await writer.drain()
        res = await reader.read(1024)
        print(f"[Scheduler] dispatch_task() - worker responded with: {res}")
        writer.close()
        await writer.wait_closed()

    async def start(self):
        # start task queue polling interval
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(every(5, self.check_task_queue))

        # start tcp server
        host = "127.0.0.1"
        port = 8700
        print(f"[Scheduler] starting tcp server on {host}:{port}")
        self.server = await asyncio.start_server(self.handle, host, port)
        async with self.server:
            await self.server.serve_forever()


async def main():
    scheduler = Scheduler()
    worker1 = WorkerInfo(host="127.0.0.1", port=8701)
    scheduler.add_worker(worker1)
    await scheduler.start()


asyncio.run(main())
