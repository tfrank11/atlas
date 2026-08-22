import asyncio

import cloudpickle
import msgpack


class Worker:
    tcp_server: asyncio.Server

    def recv(self, data: bytes):
        header_len = int.from_bytes(data[:4], "big")
        body_len = int.from_bytes(data[4:8], "big")
        header = msgpack.unpackb(data[8 : 8 + header_len])
        body_bytes = data[8 + header_len : 8 + header_len + body_len]

        try:
            task_fn = cloudpickle.loads(body_bytes)
        except Exception as e:  # noqa: BLE001
            print(f"[Worker] deserialization error={e}")
            return

        try:
            print(f"[Worker] running task_fn() from header={header}")
            task_fn()
        except Exception as e:  # noqa: BLE001
            print(f"[Worker] task error={e}")

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        print(f"connected: {peer}")
        while True:
            data = await reader.read(1024)
            if not data:
                break
            self.recv(data=data)
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
