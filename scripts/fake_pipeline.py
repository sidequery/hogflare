#!/usr/bin/env python3
# /// script
# dependencies = [
#     "fastapi",
#     "uvicorn",
#     "duckdb",
# ]
# ///

import argparse
import asyncio
import json
from typing import List

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

_CONNECTION = duckdb.connect(database=":memory:", read_only=False)
_TABLE_INITIALIZED = False
_TABLE_LOCK = asyncio.Lock()


async def _run_blocking(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


@app.post("/")
async def ingest(events: List[dict]):
    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="payload must be an array of objects")
    if not events:
        return {"status": "ok"}

    async with _TABLE_LOCK:
        def insert():
            global _TABLE_INITIALIZED
            if not _TABLE_INITIALIZED:
                _CONNECTION.execute("CREATE TABLE events (data JSON)")
                _TABLE_INITIALIZED = True

            for event in events:
                _CONNECTION.execute(
                    "INSERT INTO events VALUES (json(?))",
                    [json.dumps(event)],
                )

        await _run_blocking(insert)

    return {"status": "ok"}


@app.get("/events")
async def get_events():
    async with _TABLE_LOCK:
        def fetch():
            if not _TABLE_INITIALIZED:
                return []
            result = _CONNECTION.execute("SELECT CAST(data AS VARCHAR) FROM events")
            rows = result.fetchall()
            return [json.loads(row[0]) for row in rows]

        rows = await _run_blocking(fetch)
        return JSONResponse(rows)


@app.get("/health")
async def health():
    return {"status": "ok"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fake Cloudflare Pipeline server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="warning")
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
