import time
from io import BytesIO
from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import PlaintextValue, LiteralPlaintext, Literal, \
    Address, Value, StructPlaintext, cast, Int
from aleo_types.cached import cached_get_key_id
from db import Database

async def validators_route(request: Request):
    db: Database = request.app.state.db
    sort_tag = request.query_params.get("sort")
    if sort_tag is None:
        sort_tag = "stake"
    if sort_tag not in ["stake", "reward", "vote_power", "last_epoch_apr"]:
        raise HTTPException(status_code=400, detail="Sort Tag Error")
    if sort_tag == "reward":
        sort_tag = "staking_reward"
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
    latest_height = await db.get_latest_height()
    if latest_height is None:
        raise HTTPException(status_code=550, detail="No blocks found")
    total_validators = await db.get_validator_count_at_height(latest_height)
    if offset < 0 or offset > total_validators:
        raise HTTPException(status_code=400, detail="Invalid page")
    committee = await db.get_committee_at_height(latest_height)
    validators_data = await db.get_validators_at_height(latest_height)
    all_data: list[dict[str, Any]] = []
    for validator in validators_data:
        stake_reward = await db.get_address_stake_reward(validator["address"])
        delegate_reward = await db.get_address_delegate_reward(validator["address"])
        if stake_reward is None:
            stake_reward = 0
        if delegate_reward is None:
            delegate_reward = 0
        address_key = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads(validator["address"]),
            )
        )
        address_key_bytes = address_key.dump()
        committee_key_id = cached_get_key_id("credits.aleo", "committee", address_key_bytes)
        committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
        if committee_state_bytes is None:
            commission_value = 0 
        else:
            value = cast(PlaintextValue, Value.load(BytesIO(committee_state_bytes)))
            plaintext = cast(StructPlaintext, value.plaintext)
            commission = cast(LiteralPlaintext, plaintext["commission"])
            commission_value = int(cast(Int, commission.literal.primitive))
        last_epoch_apr_no_commission = await db.get_validatory_last_epoch_apr(validator["address"])
        all_data.append({
            "address": validator["address"],
            "address_type": "Validator",
            "staking_reward": stake_reward + delegate_reward,
            "stake": int(validator["stake"]),
            "commission": commission_value,
            "last_epoch_apr": float(last_epoch_apr_no_commission)*(1-commission_value*0.01),
            "is_open": validator["is_open"],
            "uptime": validator["uptime"] * 100,
            "vote_power": int(validator["stake"]) / int(committee["total_stake"]) * 100
        })
    sort_data = sorted(all_data, key=lambda e: e[sort_tag], reverse=True)
    if offset + limit > len(sort_data):
        data = sort_data[offset:]
    else:
        data = sort_data[offset:offset + limit]

    ctx = {
        "validators": data,
        "address_count": total_validators,
        "total_stake": int(committee["total_stake"]),
        "starting_round": int(committee["starting_round"]),
    }
    return JSONResponse(ctx)

async def validator_trending_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    current_data = int(time.time())
    today_zero_time = current_data - int(time.time() - time.timezone) % 86400
    previous_timestamp = today_zero_time - 86400 * 15

    validator_trend = await db.get_validator_daily_trend_by_address_and_time(address, previous_timestamp)
    stake_data: list[dict[str, Any]] = []
    profit_data: list[dict[str, Any]] = []
    for row in validator_trend:
        stake_data.append({
            "timestamp": row["timestamp"], "value": int(row["stake"])
        })
        profit_data.append({
            "timestamp": row["timestamp"], "value": int(row["reward"])
        })
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "stake_data": stake_data,
        "profit_data": profit_data,
    }
    return JSONResponse(ctx)

async def validator_bonds_route(request: Request):
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
    all_delegators = await db.get_validator_bonds(address)
    delegator_count = len(all_delegators)
    if offset < 0 or offset > delegator_count: 
        raise HTTPException(status_code=400, detail="Invalid page")
    all_delegators = sorted(all_delegators, key=lambda e: e['stake'], reverse=True)
    if offset + limit > len(all_delegators):
        delegators = all_delegators[offset:]
    else:
        delegators = all_delegators[offset:offset + limit]
    ctx = {
        "delegators": delegators,
        "delegator_count": delegator_count,
    }
    return JSONResponse(ctx)

async def validator_dpr_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    now = int(time.time())
    validator_trend = await db.get_validator_trend(address, now - 86400)
    dpr = sum((trend["stake_reward"]+trend["delegate_reward"])/trend["committee_stake"] for trend in validator_trend)
    address_key = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads(address),
            )
        )
    address_key_bytes = address_key.dump()
    committee_key_id = cached_get_key_id("credits.aleo", "committee", address_key_bytes)
    committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
    if committee_state_bytes is None:
        commission_value = 0 
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(committee_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        commission = cast(LiteralPlaintext, plaintext["commission"])
        commission_value = int(cast(Int, commission.literal.primitive))
    ctx = {
        "commission": commission_value,
        "daily_percentage_rate": float(dpr)*(1-commission_value*0.01),
        "validator": address,
    }
    return JSONResponse(ctx)

