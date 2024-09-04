import asyncio
import logging
import multiprocessing
import os
import time
from typing import Any

import aiohttp
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
from util.set_proc_title import set_proc_title
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


async def index_route(request: Request):
    db: Database = request.app.state.db
    recent_blocks = await db.get_recent_blocks_fast(10)
    network_speed = await db.get_network_speed(900)
    sync_info = await out_of_sync_check(request.app.state.session, db)
    latest_block = await db.get_latest_block()
    validators_count = await db.get_validator_count_at_height(latest_block.height)
    provers_count, _ = await db.get_puzzle_reward_all()
    delegators = await db.get_bonded_mapping()
    committee = await db.get_committee_at_height(latest_block.header.metadata.height)
    puzzle_reward_24H = await db.get_24H_puzzle_reward()
    block_reward_24H = await db.get_24H_block_reward()
    puzzle_reward_1M = await db.get_24H_puzzle_reward_1M()

    def get_reward(block: dict[str, Any]):
        block["reward"] = block["block_reward"] + block["coinbase_reward"]  * 2 // 3
        return block
    recent_blocks = [get_reward(block) for block in recent_blocks]
    ctx = {
        "latest_block": format_block(latest_block),
        "validators_count": validators_count,
        "provers_count": provers_count,
        "delegators_count": len(delegators) - validators_count,
        "total_stake": int(committee["total_stake"]),
        "recent_blocks": [format_number(recent_block) for recent_block in recent_blocks],
        "network_speed": str(network_speed),
        "total_reward": puzzle_reward_24H + block_reward_24H,
        "puzzle_reward": puzzle_reward_24H,
        "block_reward": block_reward_24H,
        "puzzle_reward_1M": puzzle_reward_1M,
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
    Route("/solution", solution_route),
    Route("/search", search_route),
    Route("/blocks", blocks_route),
    Route("/hashrate/{type}", hashrate_route),
    Route("/epoch", epoch_route),
    Route("/epoch_hashrate/{type}", epoch_hashrate_route),
    Route("/epoch_hash", epoch_hash_route),
    Route("/coinbase", coinbase_route),
    Route("/proof_target/{type}", proof_target_route),
    Route("/unconfirmed_transactions", unconfirmed_transactions_route),
    # Programs
    Route("/programs", programs_route),
    Route("/program", program_route),
    Route("/program/transitions", program_transitions_route),
    Route("/similar_programs", similar_programs_route),
    Route("/upload_source", upload_source_route, methods=["GET", "POST"]),
    Route("/submit_source", submit_source_route, methods=["POST"]),
    # Proving
    Route("/calc", calc_route),
    Route("/incentives", incentives_route),
    Route("/validators", validators_route),
    Route("/estimate_fee", estimate_fee_route),
    Route("/validator_dpr", validator_dpr_route),
    Route("/validator/bonds", validator_bonds_route),
    Route("/validator/trending", validator_trending_route),
    Route("/credits", credits_route),
    Route("/power/leaderboard/{type}", power_route),
    Route("/reward/leaderboard/{type}", reward_route),
    Route("/address", address_route),
    Route("/address_trending", address_trending_route),
    Route("/address_solutions", address_solution_route),
    Route("/address_transactions", address_transaction_route),
    Route("/address_function_transactions", address_function_transaction_route),
    Route("/address_favorite", address_favorite_route),
    Route("/bonds", address_bonds_transaction_route),
    Route("/transfer", address_transfer_transaction_route),
    Route("/baseline_trending", baseline_trending_route),
    Route("/favorites_update", favorites_update_route, methods=["POST"]),
    # Other
    Route("/robots.txt", robots_route),
    # mapping
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
                  redis_db=int(os.environ["REDIS_DB"]), redis_user=os.environ.get("REDIS_USER"),
                  redis_password=os.environ.get("REDIS_PASS"),
                  message_callback=noop)
    await db.connect()
    # noinspection PyUnresolvedReferences
    app.state.db = db
    app.state.program_cache = Cache()
    app.state.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1))

log_format = '\033[92mACCESS\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s \033[95m%(htmx)s\033[0m'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(CORSMiddleware, allow_origins=['*'], allow_headers=["*"], allow_methods=["*"]),
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
