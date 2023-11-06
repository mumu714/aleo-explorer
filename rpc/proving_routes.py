import time
from io import BytesIO
from typing import Any, cast

import aleo_explorer_rust
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import PlaintextValue, LiteralPlaintext, Literal, \
    Address, Value, StructPlaintext, FutureTransitionOutput, PlaintextArgument
from db import Database
from .utils import out_of_sync_check
from .format import *


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
    if offset < 0 or offset > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    leaderboard_data = await db.get_leaderboard(offset, offset + limit)
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
    if not address:
        raise HTTPException(status_code=400, detail="Missing address")
    solutions = await db.get_recent_solutions_by_address(address)
    programs = await db.get_recent_programs_by_address(address)
    transitions = await db.get_address_recent_transitions(address)
    address_key = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Address,
            primitive=Address.loads(address),
        )
    )
    address_key_bytes = address_key.dump()
    account_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "account", address_key_bytes)
    bonded_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "bonded", address_key_bytes)
    unbonded_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "unbonded", address_key_bytes)
    committee_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "committee", address_key_bytes)
    public_balance_bytes = await db.get_mapping_value("credits.aleo", "account", account_key_id)
    bond_state_bytes = await db.get_mapping_value("credits.aleo", "bonded", bonded_key_id)
    unbond_state_bytes = await db.get_mapping_value("credits.aleo", "unbonded", unbonded_key_id)
    committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
    stake_reward = await db.get_address_stake_reward(address)
    transfer_in = await db.get_address_transfer_in(address)
    transfer_out = await db.get_address_transfer_out(address)
    fee = await db.get_address_total_fee(address)

    if (len(solutions) == 0
        and len(programs) == 0
        and len(transitions) == 0
        and public_balance_bytes is None
        and bond_state_bytes is None
        and unbond_state_bytes is None
        and committee_state_bytes is None
        and stake_reward is None
        and transfer_in is None
        and transfer_out is None
        and fee is None
    ):
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
    transactions_count = await db.get_transition_count_by_address(address)
    function_names = await db.get_transition_function_name_by_address(address)
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
        deploy_info = await db.get_deploy_info_by_program_id(program)
        if deploy_info is None:
            raise HTTPException(status_code=550, detail="Deploy info not found")
        recent_programs.append({
            "program_id": program,
            "height": deploy_info["height"],
            "timestamp": deploy_info["timestamp"],
            "transaction_id": deploy_info["transaction_id"],
        })
    if public_balance_bytes is None:
        public_balance = 0
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(public_balance_bytes)))
        plaintext = cast(LiteralPlaintext, value.plaintext)
        public_balance = int(plaintext.literal.primitive)
    if bond_state_bytes is None:
        bond_state = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(bond_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        validator = cast(LiteralPlaintext, plaintext["validator"])
        amount = cast(LiteralPlaintext, plaintext["microcredits"])
        bond_state = {
            "validator": str(validator.literal.primitive),
            "amount": int(amount.literal.primitive),
        }
    if unbond_state_bytes is None:
        unbond_state = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(unbond_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        amount = cast(LiteralPlaintext, plaintext["microcredits"])
        height = cast(LiteralPlaintext, plaintext["height"])
        unbond_state = {
            "amount": int(amount.literal.primitive),
            "height": str(height.literal.primitive),
        }
    if committee_state_bytes is None:
        committee_state = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(committee_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        amount = cast(LiteralPlaintext, plaintext["microcredits"])
        is_open = cast(LiteralPlaintext, plaintext["is_open"])
        committee_state = {
            "amount": int(amount.literal.primitive),
            "is_open": bool(is_open.literal.primitive),
        }
    if stake_reward is None:
        stake_reward = 0
    if transfer_in is None:
        transfer_in = 0
    if transfer_out is None:
        transfer_out = 0
    if fee is None:
        fee = 0
    address_type = ""
    if committee_state:
        address_type = "Validator"
    elif solution_count > 0:
        address_type = "Prover"

    recent_transaction: list[dict[str, Any]] = []
    for transition_data in transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            raise HTTPException(status_code=550, detail="Transition not found")
        from_address = ""
        to_address = ""
        credit = 0
        for output in transition.outputs:
            if isinstance(output, FutureTransitionOutput):
                future = output.future.value
                if future is not None:
                    for i, argument in enumerate(future.arguments):
                        if isinstance(argument, PlaintextArgument):
                            plaintext = argument.plaintext
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.Address:
                                if i == 0:
                                    from_address = str(plaintext.literal.primitive)
                                if i == 1:
                                    to_address = str(plaintext.literal.primitive)
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.U64:
                                credit = format_aleo_credit(plaintext.literal.primitive) # type: ignore
        state = ""
        if transition_data["type"].startswith("Accepted"):
            state = "Accepted"
        elif transition_data["type"].startswith("Rejected"):
            state = "Rejected"
        recent_transaction.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"],
            "timestamp": transition_data["timestamp"],
            "transaction_id": transition_data["transaction_id"],
            "from": from_address,
            "to": to_address, 
            "credit": credit, 
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })
    
    sync_info = await out_of_sync_check(db)
    network_1hour_speed = await db.get_network_speed(3600)
    network_1hour_reward = await db.get_network_reward(3600)
    address_1hour_reward = await db.get_address_reward(address, 3600)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "address_type": address_type,
        "total_rewards": int(total_rewards),
        "total_incentive": int(total_incentive),
        "total_solutions": solution_count,
        "total_programs": program_count,
        "total_transactions": transactions_count,
        "function_names": function_names,
        "speed": float(speed),
        "timespan": interval_text[interval],
        "public_balance": public_balance,
        "bond_state": bond_state,
        "unbond_state": unbond_state,
        "committee_state": committee_state,
        "stake_reward": stake_reward,
        "transfer_in": transfer_in,
        "transfer_out": transfer_out,
        "fee": fee,
        "address_1hour_reward": int(address_1hour_reward),
        "network": {
            "network_1hour_speed": float(network_1hour_speed),
            "network_1hour_reward": int(network_1hour_reward)

        },
        "solutions": recent_solutions,
        "programs": recent_programs,
        "transaction": recent_transaction,
        "sync_info": sync_info,
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
    if offset < 0 or offset > solution_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    solutions = await db.get_solution_by_address(address, offset, offset + limit)
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

async def address_transaction_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
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
    transactions_count = await db.get_transition_count_by_address(address)
    if offset < 0 or offset > transactions_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    transitions = await db.get_transition_by_address(address, offset, offset + limit)
    data: list[dict[str, Any]] = []
    for transition_data in transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            raise HTTPException(status_code=550, detail="Transition not found")
        from_address = ""
        to_address = ""
        credit = 0
        for output in transition.outputs:
            if isinstance(output, FutureTransitionOutput):
                future = output.future.value
                if future is not None:
                    for i, argument in enumerate(future.arguments):
                        if isinstance(argument, PlaintextArgument):
                            plaintext = argument.plaintext
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.Address:
                                if i == 0:
                                    from_address = str(plaintext.literal.primitive)
                                if i == 1:
                                    to_address = str(plaintext.literal.primitive)
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.U64:
                                credit = format_aleo_credit(plaintext.literal.primitive) # type: ignore
        state = ""
        if transition_data["type"].startswith("Accepted"):
            state = "Accepted"
        elif transition_data["type"].startswith("Rejected"):
            state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"],
            "timestamp": transition_data["timestamp"],
            "transaction_id": transition_data["transaction_id"],
            "from": from_address,
            "to": to_address, 
            "credit": credit, 
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })
    
    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "transaction_count": transactions_count,
        "transactions": data,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)

async def address_function_transaction_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    function = request.query_params.get("f")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    if function is None:
        raise HTTPException(status_code=400, detail="Missing function")
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
    transactions_count = await db.get_transition_count_by_address_and_function(address, function)
    if offset < 0 or offset > transactions_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    transitions = await db.get_transition_by_address_and_function(address, function, offset, offset + limit)
    data: list[dict[str, Any]] = []
    for transition_data in transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            raise HTTPException(status_code=550, detail="Transition not found")
        from_address = ""
        to_address = ""
        credit = 0
        for output in transition.outputs:
            if isinstance(output, FutureTransitionOutput):
                future = output.future.value
                if future is not None:
                    for i, argument in enumerate(future.arguments):
                        if isinstance(argument, PlaintextArgument):
                            plaintext = argument.plaintext
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.Address:
                                if i == 0:
                                    from_address = str(plaintext.literal.primitive)
                                if i == 1:
                                    to_address = str(plaintext.literal.primitive)
                            if isinstance(plaintext, LiteralPlaintext) and plaintext.literal.type == Literal.Type.U64:
                                credit = format_aleo_credit(plaintext.literal.primitive) # type: ignore
        state = ""
        if transition_data["type"].startswith("Accepted"):
            state = "Accepted"
        elif transition_data["type"].startswith("Rejected"):
            state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"],
            "timestamp": transition_data["timestamp"],
            "transaction_id": transition_data["transaction_id"],
            "from": from_address,
            "to": to_address, 
            "credit": credit, 
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })
    
    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "transaction_count": transactions_count,
        "transactions": data,
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
    if offset < 0 or offset > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    address_hashrate = await db.get_15min_top_miner(offset, offset + limit)
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