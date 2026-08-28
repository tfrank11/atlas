import asyncio
from collections import deque
from dataclasses import dataclass

from atlas.lib.asyncio_utils import every
from atlas.lib.protocol_utils import deserialize_header, read_frame, serialize_request
from atlas.lib.types import (
    ClientSubmitTask,
    RequestType,
    SchedulerAckTask,
    SchedulerAckWorkerLogin,
)


@dataclass
class WorkerInfo:
    host: str
    port: int
    writer: asyncio.StreamWriter


@dataclass
class TaskInfo:
    task_id: int
    data: bytes
    retries: int = 0


class Scheduler:
    tcp_server: asyncio.Server

    workers: list[WorkerInfo]
    task_queue: deque[TaskInfo]
    max_retries: int

    def __init__(self, max_retries: int = 5):
        self.task_queue = deque()
        self.workers = []
        self.max_retries = max_retries

    async def check_task_queue(self):
        if not self.task_queue:
            return
        print(f"[Scheduler] check_task_queue -> see {len(self.task_queue)} tasks")
        if not self.workers:
            print("[Scheduler] check_task_queue -> no workers available")
        for worker in self.workers:
            if not self.task_queue:
                break
            task = self.task_queue.popleft()
            ok = await self.dispatch_task(task=task, worker=worker)
            if not ok:
                if task.retries > self.max_retries:
                    print(
                        f"[Scheduler] task (task_id={task.task_id}) retries ({task.retries}) exceeded max ({self.max_retries}). Giving up."
                    )
                    break
                task.retries += 1
                self.task_queue.append(task)

    async def dispatch_task(self, worker: WorkerInfo, task: TaskInfo) -> bool:
        print(
            f"[Scheduler] dispatching task (task_id={task.task_id}) to worker ({worker.host}:{worker.port})"
        )
        try:
            writer = worker.writer
            writer.write(data=task.data)
            await writer.drain()
            # res = await reader.read(1024)
            # print(f"[Scheduler] dispatch_task() - worker responded with: {res}")
            return True
        except Exception as e:  # noqa: BLE001
            print(f"[Scheduler] task dispatch error, adding back to queue. {e}")
            return False

    async def handle_submit_task(
        self,
        writer: asyncio.StreamWriter,
        header: ClientSubmitTask,
        full_payload: bytes,
    ):
        print(f"[Scheduler] received task_id={header.task_id}")
        task = TaskInfo(task_id=header.task_id, data=full_payload)
        self.task_queue.append(task)
        await self.check_task_queue()
        header = SchedulerAckTask()
        payload = serialize_request(header)
        writer.write(payload)
        await writer.drain()

    async def handle_worker_login(self, writer: asyncio.StreamWriter):
        client_address = writer.get_extra_info("peername")
        host, port = client_address
        worker = WorkerInfo(host=host, port=port, writer=writer)
        self.workers.append(worker)
        header = SchedulerAckWorkerLogin()
        payload = serialize_request(header)
        writer.write(payload)
        await writer.drain()
        print(f"[Scheduler] accepting new worker at {host}:{port}")

    async def handle_tcp(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peer = writer.get_extra_info("peername")
        print(f"[Scheduler] client connected: {peer}")
        while True:
            frame = await read_frame(reader)
            if not frame:
                break
            header = deserialize_header(frame.header_bytes)
            if header.type == RequestType.CLIENT_SUBMIT_TASK:
                await self.handle_submit_task(
                    writer=writer,
                    header=header,
                    full_payload=frame.full_payload_bytes,
                )
            elif header.type == RequestType.WORKER_LOGIN:
                await self.handle_worker_login(writer=writer)
            elif header.type == RequestType.WORKER_TASK_FINISHED:
                print("[Scheduler] worker finished task")
            else:
                print(f"[Scheduler] received unexpected header: {header}")
        print(f"[Scheduler] client disconnected: {peer}")
        writer.close()

    async def start(self):
        # start task queue polling interval
        event_loop = asyncio.get_event_loop()
        event_loop.create_task(every(5, self.check_task_queue))

        # start tcp server
        host = "127.0.0.1"
        port = 8700
        print(f"[Scheduler] starting tcp server on {host}:{port}")
        self.server = await asyncio.start_server(self.handle_tcp, host, port)
        async with self.server:
            await self.server.serve_forever()


async def main():
    scheduler = Scheduler()
    await scheduler.start()


asyncio.run(main())
