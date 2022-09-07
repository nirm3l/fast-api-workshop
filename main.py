from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse
from aiohttp_sse_client import client as sse_client
from threading import Thread
from collections import deque
from pydantic import ValidationError
from typing import Callable, Optional

from wiki.schemas import WikiRecord

import uvicorn, asyncio, logging

app = FastAPI()

wiki_records = deque(maxlen=100)

logger = logging.getLogger(__name__)

WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream'


@app.on_event("startup")
async def startup_event() -> None:
    async def load_wiki_records():
        async with sse_client.EventSource(WIKI_STREAM_URL + '/recentchange') as event_source:
            try:
                async for event in event_source:
                    try:
                        wiki_records.append(WikiRecord.parse_raw(event.data))
                    except ValidationError as e:
                        logger.warning(f'Missing value {e}')

                        pass
            except ConnectionError as e:
                logger.error(f'Connection error {e}')

                pass

    thread = Thread(target=asyncio.run, args=(load_wiki_records(),))
    thread.start()


async def get_events(
        f: Callable[[WikiRecord], str] = lambda x: x,
        distinct: bool = False,
        condition: Optional[Callable[[WikiRecord], bool]] = None):
    async def event_generator():
        last_event = None

        duplication_check = set()

        while True:
            if wiki_records and last_event is not wiki_records[-1]:
                start_index = 0

                records = list(wiki_records)

                if last_event is not None:
                    try:
                        start_index = records.index(last_event)
                    except ValueError:
                        pass

                for index in range(start_index, len(records)):
                    if condition is None or condition(records[index]):
                        value = f(records[index])

                        if distinct:
                            if value in duplication_check:
                                continue

                            duplication_check.add(value)

                        yield {
                            "event": "data",
                            "data": f(records[index])
                        }

                    last_event = records[index]

            await asyncio.sleep(10e-3)

    return EventSourceResponse(event_generator())


@app.get("/wikis")
async def titles():
    return await get_events(lambda x: x.wiki, distinct=True)


@app.get("/titles")
async def titles():
    return await get_events(lambda x: x.title)


@app.get("/titles/{wiki}")
async def root(wiki: str):
    return await get_events(lambda x: x.title, condition=lambda x: x.wiki == wiki)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
