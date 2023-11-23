import asyncio
import logging
import multiprocessing
import os
import time
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles
from starlette.responses import JSONResponse
from starlette.routing import Route

from api.execute_routes import preview_finalize_route
from api.mapping_routes import mapping_route, mapping_list_route, mapping_value_list_route
from db import Database
from middleware.api_filter import APIFilterMiddleware
from middleware.api_quota import APIQuotaMiddleware
from middleware.asgi_logger import AccessLoggerMiddleware
from starlette.middleware.cors import CORSMiddleware
from middleware.api_quota import APIQuotaMiddleware
from middleware.server_timing import ServerTimingMiddleware
from util.cache import Cache
from middleware.minify import MinifyMiddleware
# from node.light_node import LightNodeState
from .chain_routes import *
from .program_routes import *
from .proving_routes import *
from .utils import out_of_sync_check
from .format import *


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args: Any, **kwargs: Any):
        self.server.run()

async def commitment_route(request: Request):
    db: Database = request.app.state.db
    if time.time() >= 1675209600:
        return JSONResponse(None)
    commitment = request.query_params.get("commitment")
    if not commitment:
        return HTTPException(400, "Missing commitment")
    return JSONResponse(await db.get_puzzle_commitment(commitment))

async def index_route(request: Request):
    db: Database = request.app.state.db
    recent_blocks = await db.get_recent_blocks_fast()
    network_speed = await db.get_network_speed(900)
    sync_info = await out_of_sync_check(db)
    latest_block = await db.get_latest_block()
    ctx = {
        "latest_block": format_block(latest_block),
        "recent_blocks": [format_number(recent_block) for recent_block in recent_blocks],
        "network_speed": str(network_speed),
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)

async def robots_route(_: Request):
    return FileResponse("rpc/robots.txt", headers={'Cache-Control': 'public, max-age=3600'})


routes = [
    Route("/", index_route),
    # Blockchain
    Route("/block", block_route),
    Route("/block_solutions", block_solution_route),
    Route("/transaction", transaction_route),
    Route("/transactions", transactions_route),
    Route("/transition", transition_route),
    Route("/transitions", transitions_route),
    Route("/search", search_route),
    Route("/blocks", blocks_route),
    Route("/hashrate", hashrate_route),
    Route("/coinbase", coinbase_route),
    # Programs
    Route("/programs", programs_route),
    Route("/program", program_route),
    Route("/similar_programs", similar_programs_route),
    Route("/upload_source", upload_source_route, methods=["GET", "POST"]),
    Route("/submit_source", submit_source_route, methods=["POST"]),
    # Proving
    Route("/calc", calc_route),
    Route("/validators", validators_route),
    Route("/leaderboard", leaderboard_route),
    Route("/address", address_route),
    Route("/address_solutions", address_solution_route),
    Route("/address_transactions", address_transaction_route),
    Route("/address_function_transactions", address_function_transaction_route),
    Route("/biggest_miners", biggest_miners_route),
    # Other
    Route("/robots.txt", robots_route),
    # mapping
    Route("/commitment", commitment_route),
    Route("/v{version:int}/mapping/get_value/{program_id}/{mapping}/{key}", mapping_route),
    Route("/v{version:int}/mapping/list_program_mappings/{program_id}", mapping_list_route),
    Route("/v{version:int}/mapping/list_program_mapping_values/{program_id}/{mapping}", mapping_value_list_route),
    Route("/v{version:int}/preview_finalize_execution", preview_finalize_route, methods=["POST"]),
]

async def startup():
    async def noop(_: Any): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  redis_server=os.environ["REDIS_HOST"], redis_port=int(os.environ["REDIS_PORT"]),
                  redis_db=int(os.environ["REDIS_DB"]),
                  message_callback=noop)
    await db.connect()
    # noinspection PyUnresolvedReferences
    app.state.db = db
    app.state.program_cache = Cache()


log_format = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s \033[95m%(htmx)s\033[0m'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(CORSMiddleware, allow_origins=['*'], allow_headers=["*"]),
        Middleware(ServerTimingMiddleware),
        Middleware(MinifyMiddleware),
        # Middleware(APIQuotaMiddleware),
        Middleware(APIFilterMiddleware),
    ]
)

async def run():
    host = os.environ.get("RPC_HOST", "127.0.0.1")
    port = int(os.environ.get("RPC_PORT", 8002))
    config = uvicorn.Config("rpc:app", reload=True, log_level="info", host=host, port=port)
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)

async def run_profile():
    config = uvicorn.Config("rpc:app", reload=True, log_level="info", port=8889)
    await uvicorn.Server(config).serve()
