from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse
from aiohttp_sse_client import client as sse_client
from threading import Thread
from collections import deque
from pydantic import ValidationError
from typing import Callable, Optional
from aiohttp import ClientTimeout

from wiki.schemas import WikiRecord
# from pyctuator.pyctuator import Pyctuator

import asyncio, logging, aiohttp

app = FastAPI()

wiki_records = deque(maxlen=100)

logger = logging.getLogger(__name__)

WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream'


@app.on_event("startup")
async def startup_event() -> None:
    async def load_wiki_records():
        while True:
            try:
                async with sse_client.EventSource(
                        WIKI_STREAM_URL + '/recentchange', timeout=ClientTimeout(sock_read=0)) as event_source:
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
            except aiohttp.client.ClientPayloadError as e:
                logger.error(f'error {e}, retry!')

    thread = Thread(target=asyncio.run, args=(load_wiki_records(),))
    thread.start()


def get_new_records(wiki_records: deque, last_event: WikiRecord):
    records_copy = list(wiki_records)

    records = []

    for value in reversed(records_copy):
        if value == last_event:
            break
        records.append(value)

    return reversed(records)


async def get_events(f: Callable[[WikiRecord], str] = lambda x: x, distinct: bool = False,
                     condition: Optional[Callable[[WikiRecord], bool]] = None):
    async def event_generator():
        last_event = None

        duplication_check = set()

        while True:
            if wiki_records and last_event is not wiki_records[-1]:
                for record in get_new_records(wiki_records, last_event):
                    if condition is None or condition(record):
                        value = f(record)

                        if distinct:
                            if value in duplication_check:
                                continue

                            duplication_check.add(value)

                        yield {
                            "event": "data",
                            "data": f(record)
                        }

                    last_event = record

            await asyncio.sleep(100e-3)

    return EventSourceResponse(event_generator())


@app.get("/wikis")
async def wikis():
    return await get_events(lambda x: x.wiki, distinct=True)


@app.get("/titles")
async def titles():
    return await get_events(lambda x: x.title)


@app.get("/titles/{wiki}")
async def wiki_titles(wiki: str):
    return await get_events(lambda x: x.title, condition=lambda x: x.wiki == wiki)

#Pyctuator(
#    app,
#    "FastAPI Workshop",
#    app_url="http://localhost:8000",
#    pyctuator_endpoint_url="http://localhost:8000/pyctuator",
#    registration_url="http://localhost:8080/instances"
#)
