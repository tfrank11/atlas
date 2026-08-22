import asyncio


async def every(seconds: float, func, *args, **kwargs):
    while True:
        await func(*args, **kwargs)
        await asyncio.sleep(seconds)
