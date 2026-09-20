# Atlas

Atlas is a basic distributed compute framework, allowing a client to run python code on worker nodes. 

Clients send tasks to the scheduler, which puts them in a queue to be dispatched to the next available worker. Workers register/unregister themselves with the scheduler whenever (doesnt need to be at startup). 

### Example Client Code

```python
import asyncio
from atlas.client import Client


async def main():
    client = Client(scheduler_host="x.x.x.x", scheduler_port=8700)

    def task():
        # run expensive task
        return xxxx

    res = await client.run(task)
    print(f"res={res}")


asyncio.run(main())
```


### Usage
Install deps
```
uv sync
```

start scheduler:
```
uv run scheduler
```

or start worker(s): 
```
uv run worker
```

or run example client code:
```
uv run client
```