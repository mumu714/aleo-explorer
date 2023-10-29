import time
from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import Transaction, AcceptedDeploy, DeployTransaction
from db import Database
from .utils import out_of_sync_check


async def calc_route(request: Request):
    db: Database = request.app.state.db
    proof_target = (await db.get_latest_block()).header.metadata.proof_target
    ctx = {
        "proof_target": proof_target,
    }
    return JSONResponse(ctx)


async def leaderboard_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 50
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    address_count = await db.get_leaderboard_size()
    if offset < 0 or offset + limit > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    leaderboard_data = await db.get_leaderboard(start, start + limit)
    data: list[dict[str, Any]] = []
    for line in leaderboard_data:
        data.append({
            "address": line["address"],
            "total_rewards": str(int(line["total_reward"])),
            "total_incentive": str(int(line["total_incentive"])),
        })
    now = int(time.time())
    total_credit = await db.get_leaderboard_total()
    target_credit = 37_500_000_000_000
    ratio = total_credit / target_credit * 100
    sync_info = await out_of_sync_check(db)
    ctx = {
        "leaderboard": data,
        "address_count": address_count,
        "total_credit": total_credit,
        "target_credit": target_credit,
        "ratio": ratio,
        "now": now,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)

async def address_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    solutions = await db.get_recent_solutions_by_address(address)
    programs = await db.get_recent_programs_by_address(address)
    address_info = await db.get_address_info(address)
    address_transactions = await db.get_recent_transactions_by_address(address)
    if address_info is None:
        raise HTTPException(status_code=404, detail="Address not found")
    if len(solutions) > 0:
        solution_count = await db.get_solution_count_by_address(address)
        total_rewards, total_incentive = await db.get_leaderboard_rewards_by_address(address)
        speed, interval = await db.get_address_speed(address)
    else:
        solution_count = 0
        total_rewards = 0
        total_incentive = 0
        speed = 0
        interval = 0
    program_count = await db.get_program_count_by_address(address)
    interval_text = {
        0: "never",
        900: "15 minutes",
        1800: "30 minutes",
        3600: "1 hour",
        14400: "4 hours",
        43200: "12 hours",
        86400: "1 day",
    }
    recent_solutions: list[dict[str, Any]] = []
    for solution in solutions:
        recent_solutions.append({
            "height": solution["height"],
            "timestamp": solution["timestamp"],
            "reward": solution["reward"],
            "nonce": str(solution["nonce"]),
            "target": str(solution["target"]),
            "target_sum": str(solution["target_sum"]),
            "commitment": str(solution["commitment"])
        })
    recent_programs: list[dict[str, Any]] = []
    for program in programs:
        program_tx: DeployTransaction | None = None
        program_block = await db.get_block_by_program_id(program)
        if program_block is None:
            raise HTTPException(status_code=550, detail="Program block not found")
        for ct in program_block.transactions.transactions:
            if isinstance(ct, AcceptedDeploy):
                tx = ct.transaction
                if isinstance(tx, DeployTransaction) and tx.type == Transaction.Type.Deploy and str(tx.deployment.program.id) == program:
                    program_tx = tx
                    break
        if program_tx is None:
            raise HTTPException(status_code=550, detail="Program transaction not found")
        recent_programs.append({
            "program_id": program,
            "height": program_block.header.metadata.height,
            "timestamp": program_block.header.metadata.timestamp,
            "transaction_id": program_tx.id,
        })
    recent_transaction: list[dict[str, Any]] = []
    for transaction in address_transactions:
        recent_transaction.append({
            "height": transaction["height"],
            "timestamp": transaction["timestamp"],
            "transaction_id": transaction["transaction_id"],
            "from": transaction["from_address"],
            "to": transaction["to_address"], 
            "credit": int(transaction["credit"]), 
            "state": transaction["state"],
            "function_name": transaction["function_name"]
        })
    sync_info = await out_of_sync_check(db)
    network_1hour_speed = await db.get_network_speed(3600)
    network_1hour_reward = await db.get_network_reward(3600)
    address_1hour_reward = await db.get_address_reward(address, 3600)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "total_credit": int(address_info["total_credit"]),
        "type": address_info["type"],
        "owner_staking": address_info["owner_staking"],
        "delegators_staking": address_info["delegators_staking"],
        "if_open": address_info["if_open"],
        "total_rewards": int(total_rewards),
        "total_incentive": int(total_incentive),
        "total_solutions": solution_count,
        "total_programs": program_count,
        "speed": float(speed),
        "timespan": interval_text[interval],
        "sync_info": sync_info,
        "address_1hour_reward": int(address_1hour_reward),
        "network": {
            "network_1hour_speed": float(network_1hour_speed),
            "network_1hour_reward": int(network_1hour_reward)

        },
        "solutions": recent_solutions,
        "programs": recent_programs,
        "transaction": recent_transaction
    }

    return JSONResponse(ctx)


async def address_solution_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 50
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    solution_count = await db.get_solution_count_by_address(address)
    if offset < 0 or offset + limit > solution_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    solutions = await db.get_solution_by_address(address, start, start + limit)
    data: list[dict[str, Any]] = []
    for solution in solutions:
        data.append({
            "height": solution["height"],
            "timestamp": solution["timestamp"],
            "reward": solution["reward"],
            "nonce": str(solution["nonce"]),
            "target": str(solution["target"]),
            "target_sum": str(solution["target_sum"]),
            "commitment": solution["commitment"]
        })
    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "solutions": data,
        "solution_count": solution_count,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)


async def biggest_miners_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 4
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    address_count = await db.get_leaderboard_size()
    if offset < 0 or offset + limit > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    address_hashrate = await db.get_15min_top_miner(start, start + limit)
    data: list[dict[str, Any]] = []
    for line in address_hashrate:
        data.append({
            "address": line["address"],
            "timestamp": line["timestamp"],
            "hashrate": str(line["hashrate"])
        })
    ctx = {
        "address_15min_hashrate": data
    }

    return JSONResponse(ctx)