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
    WorkerFinishedTask,
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
    client_writer: asyncio.StreamWriter
    retries: int = 0


class Scheduler:
    tcp_server: asyncio.Server

    workers: dict[(str, int), WorkerInfo]  # key = (host, port)
    task_queue: deque[TaskInfo]
    cur_tasks: dict[str, TaskInfo]  # key = task_id

    max_retries: int

    def __init__(self, max_retries: int = 5):
        self.workers = {}
        self.task_queue = deque()
        self.cur_tasks = {}
        self.max_retries = max_retries

    async def check_task_queue(self):
        if not self.task_queue:
            return
        print(f"[Scheduler] check_task_queue -> see {len(self.task_queue)} tasks")
        if not self.workers:
            print("[Scheduler] check_task_queue -> no workers available")
        for worker in self.workers.values():
            if not self.task_queue:
                break
            task = self.task_queue.popleft()
            ok = await self.dispatch_task(task=task, worker=worker)
            if not ok:
                if task.retries >= self.max_retries:
                    print(
                        f"[Scheduler] task (task_id={task.task_id}) retries ({task.retries}) is at max ({self.max_retries}). Giving up."
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
            self.cur_tasks[task.task_id] = task
            writer.write(data=task.data)
            await writer.drain()
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
        task = TaskInfo(task_id=header.task_id, data=full_payload, client_writer=writer)
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
        worker_key = (host, port)
        self.workers[worker_key] = worker
        header = SchedulerAckWorkerLogin()
        payload = serialize_request(header)
        writer.write(payload)
        await writer.drain()
        print(f"[Scheduler] accepting new worker at {host}:{port}")

    async def handle_worker_task_finished(
        self,
        writer: asyncio.StreamWriter,
        header: WorkerFinishedTask,
        full_payload_bytes: bytes,
    ):
        client_address = writer.get_extra_info("peername")
        host, port = client_address
        client_writer = self.cur_tasks[header.task_id].client_writer
        client_writer.write(full_payload_bytes)
        await client_writer.drain()
        self.cur_tasks.pop(header.task_id)
        print(f"[Scheduler] worker ({host}:{port}) finished task ({header.task_id})")

    async def handle_tcp(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peer = writer.get_extra_info("peername")
        print(f"[Scheduler] tcp client connected: {peer}")
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
                await self.handle_worker_task_finished(
                    writer=writer,
                    header=header,
                    full_payload_bytes=frame.full_payload_bytes,
                )
            else:
                print(f"[Scheduler] received unexpected header: {header}")
        print(f"[Scheduler] tcp client disconnected: {peer}")
        host, port = peer
        peer_key = (host, port)
        if peer_key in self.workers:
            self.workers.pop(peer_key)
            print(f"[Scheduler] worker unexpectedly disconnected ({peer})")
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
