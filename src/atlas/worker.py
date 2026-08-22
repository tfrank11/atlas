import asyncio

import cloudpickle
import msgpack

from atlas.lib.reader_utils import read_frame


class Worker:
    tcp_server: asyncio.Server

    def recv(self, header: bytes, body: bytes):
        header = msgpack.unpackb(header)
        print(f"[Worker] received header={header}")

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
        except Exception as e:  # noqa: BLE001
            print(f"[Worker] task error={e}")
            print("[Worker] ----------------------------------------")

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        print(f"connected: {peer}")
        while True:
            frame = await read_frame(reader)
            if not frame:
                break
            self.recv(header=frame.header, body=frame.body)
            writer.write(b"ack")
            await writer.drain()
        print(f"disconnected: {peer}")
        writer.close()

    async def start(self):
        host = "127.0.0.1"
        port = 8701
        print(f"[Worker] starting tcp server on {host}:{port}")
        self.tcp_server = await asyncio.start_server(self.handle, host, port)
        async with self.tcp_server:
            await self.tcp_server.serve_forever()


async def main():
    worker = Worker()
    await worker.start()


asyncio.run(main())
