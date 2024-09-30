from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from db import Database

async def hashrate_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    hashrate_data = await db.get_hashrate(interval[type])
    data: list[dict[str, Any]] = []
    for line in hashrate_data:
        data.append({
            "timestamp": line["timestamp"],
            "hashrate": str(line["hashrate"])
            })
    ctx = {
        "hashrate": data,
    }
    return JSONResponse(ctx)

async def epoch_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 10
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    last_height = await db.get_latest_height()
    if last_height is None:
        raise NotImplementedError
    cur_epoch_num = last_height // 360
    cur_epoch =  await db.get_cur_epoch(cur_epoch_num)
    if offset > cur_epoch_num:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    epoch_data = await db.get_epoch(start, start + limit)
    ctx = {
        "last_height": last_height,
        "epoch_num": cur_epoch_num,
        "start_timestamp": cur_epoch["timestamp"],
        "epoch_hash": cur_epoch["previous_hash"],
        "epoch_data": epoch_data,
    }
    return JSONResponse(ctx)

async def epoch_hashrate_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    epoch_hashrate_data = await db.get_epoch_hashrate(interval[type])
    data: list[dict[str, Any]] = []
    for line in epoch_hashrate_data:
        data.append({
            "height": line["height"],
            "timestamp": line["timestamp"],
            "hashrate": str(line["hashrate"])
            })
    ctx = {
        "epoch_hashrate": data,
    }
    return JSONResponse(ctx)

async def epoch_hash_route(request: Request):
    db: Database = request.app.state.db
    epoch_hashrate_data = await db.get_epoch_hash()
    ctx = {
        "epoch_hash": epoch_hashrate_data,
    }
    return JSONResponse(ctx)

async def coinbase_route(request: Request):
    db: Database = request.app.state.db
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        raise HTTPException(status_code=550, detail="No blocks found")
    coinbases = await db.get_coinbase()
    data: list[dict[str, Any]] = []
    for coinbase in coinbases:
        staking_reward = int(coinbase["block_reward"])
        puzzle_reward = int(coinbase["reward"] * 2 // 3)
        data.append({
            "height": coinbase["height"],
            "timestamp": coinbase["timestamp"],
            "staking_reward": staking_reward,
            "puzzle_reward": puzzle_reward,
            "total_reward": staking_reward + puzzle_reward,
            "reward": int(coinbase["reward"])
        })
    ctx = {
        "coinbase": data,
    }
    return JSONResponse(ctx)

async def puzzle_rewards_1M_route(request: Request):
    db: Database = request.app.state.db
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        raise HTTPException(status_code=550, detail="No blocks found")
    all_data = await db.get_puzzle_rewards_1M()
    data: list[dict[str, Any]] = []
    for one_day_data in all_data:
        data.append({
            "reward": float(one_day_data["puzzle_rewards_1m"]),
            "timestamp": one_day_data["timestamp"]
        })
    ctx = {
        "puzzle_rewards_1M": data,
    }
    return JSONResponse(ctx)

async def proof_target_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    proof_targets = await db.get_interval_proof_target(interval[type])
    if not proof_targets:
        raise HTTPException(status_code=550, detail="No blocks found")
    data: list[dict[str, Any]] = []
    for proof_target in proof_targets:
        data.append({
            "height": proof_target["height"],
            "proof_target": int(proof_target["proof_target"]),
            "timestamp": proof_target["timestamp"],
        })
    ctx = {
        "proof_targets": data,
    }
    return JSONResponse(ctx)
