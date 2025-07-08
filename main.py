import time
import threading
from queue import Queue
import asyncio
from collections import defaultdict


class Broker:

    def __init__(self):
        self.subscribers = defaultdict(list)
        self.lock = threading.Lock()

    async def publish(self, topic: str, msg: str) -> None:
        with self.lock:
            if topic not in self.subscribers:
                return
            for queue in list(self.subscribers[topic]):
                await queue.put(msg)

    async def subscribe(self, topic: str) -> asyncio.Queue:
        queue = asyncio.Queue()
        with self.lock:
            self.subscribers[topic].append(queue)
        return queue

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        if topic in self.subscribers:
            self.subscribers[topic].remove(queue)
            if not self.subscribers[topic]:
                del self.subscribers[topic]


async def pub(broker: Broker, topic: str):
    for i in range(5):
        await asyncio.sleep(1)
        await broker.publish(topic, f"Message {i} on {topic}")


async def sub(broker: Broker, topic: str, name: str):
    queue = await broker.subscribe(topic)
    while True:
        msg = await queue.get()
        if msg is None:  # Exit condition
            break
        print(f"{name} received: {msg}")
        queue.task_done()


async def main():
    broker = Broker()
    asyncio.create_task(sub(broker, "news", "Alice"))
    asyncio.create_task(sub(broker, "news", "Bob"))
    await pub(broker, "news")


if __name__ == "__main__":
    asyncio.run(main())
