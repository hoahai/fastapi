from __future__ import annotations

import time
from fastapi import Request


async def timing_middleware(request: Request, call_next):
    # Set start time BEFORE route executes
    request.state.start_time = time.perf_counter()

    response = await call_next(request)

    return response
