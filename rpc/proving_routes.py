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
from .classes import UIAddress
from .utils import out_of_sync_check, get_address_type
from .format import *
from aleo_types import *


async def calc_route(request: Request):
    db: Database = request.app.state.db
    proof_target = (await db.get_latest_block()).header.metadata.proof_target
    sync_info = await out_of_sync_check(db)
    ctx = {
        "proof_target": proof_target,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)

async def validators_route(request: Request):
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
    address_count = await db.get_validators_size()
    if offset < 0 or offset > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    committee, validators = await db.get_validators(offset, offset + limit)
    data: list[dict[str, Any]] = []
    for validator in validators:
        address_type = await get_address_type(db, validator["address"])
        stake_reward = await db.get_address_stake_reward(validator["address"])
        delegate_reward = await db.get_address_delegate_reward(validator["address"])
        if stake_reward is None:
            stake_reward = 0
        if delegate_reward is None:
            delegate_reward = 0
        data.append({
            "address": validator["address"],
            "address_type": address_type,
            "staking_reward": stake_reward + delegate_reward,
            "stake": int(validator["stake"]),
            "is_open": validator["is_open"]
        })
    sync_info = await out_of_sync_check(db)
    ctx = {
        "validators": data,
        "address_count": address_count,
        "total_stake": int(committee["total_stake"]),
        "starting_round": int(committee["starting_round"]),
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)

async def credits_route(request: Request):
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
    address_count = await db.get_credits_leaderboard_size()
    if offset < 0 or offset > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    credits_leaderboard = await db.get_credits_leaderboard(offset, offset + limit)
    data: list[dict[str, Any]] = []
    for line in credits_leaderboard:
        total_rewards, _ = await db.get_leaderboard_rewards_by_address(line["address"])
        address_type = await get_address_type(db, line["address"])
        if address_type == "Validator":
            stake_reward = await db.get_address_stake_reward(line["address"])
            delegate_reward = await db.get_address_delegate_reward(line["address"])
            if stake_reward is None:
                stake_reward = 0
            if delegate_reward is None:
                delegate_reward = 0
            total_rewards += stake_reward + delegate_reward
        data.append({
            "address": line["address"],
            "address_type": address_type,
            "public_credits": int(line["public_credits"]),
            "total_reward": int(total_rewards)
        })
    sync_info = await out_of_sync_check(db)
    ctx = {
        "leaderboard": data,
        "address_count": address_count,
        "sync_info": sync_info,
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


async def reward_route(request: Request):
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
    type = request.path_params["type"]
    interval = {
        "15min": 900,
        "1h": 3600,
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    now = int(time.time())
    all_data: list[dict[str, Any]] = []
    data: list[dict[str, Any]] = []
    if type == "all":
        address_count = await db.get_leaderboard_size()
        if offset < 0 or offset > address_count:
            raise HTTPException(status_code=400, detail="Invalid page")
        leaderboard_data = await db.get_leaderboard(offset, offset + limit)
        for line in leaderboard_data:
            address_type = await get_address_type(db, line["address"])
            data.append({
                "address": line["address"],
                "address_type": address_type,
                "reward": int(line["total_reward"]),
                "total_reward": int(line["total_reward"]),
            })
    else:
        solutions = await db.get_solutions_by_time(now - interval[type])
        address_list = list(set(map(lambda x: x['address'], solutions)))
        address_count = len(address_list)
        if offset < 0 or offset > address_count:
            raise HTTPException(status_code=400, detail="Invalid page")
        for address in address_list:
            cur_solution = [solution for solution in solutions if solution["address"] == address]
            total_rewards, _ = await db.get_leaderboard_rewards_by_address(address)
            address_type = await get_address_type(db, address)
            all_data.append({
                "address": address,
                "address_type": address_type,
                "reward": sum(solution["reward"] for solution in cur_solution),
                "count": len(cur_solution),
                "total_reward": int(total_rewards)
            })
        leaderboard_data = sorted(all_data, key=lambda e: e['reward'], reverse=True)
        if offset + limit > len(leaderboard_data):
            data = leaderboard_data[offset:]
        else:
            data = leaderboard_data[offset:offset + limit]

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


async def power_route(request: Request):
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
    type = request.path_params["type"]
    interval = {
        "15min": 900,
        "1h": 3600,
        "1d": 86400,
        "7d": 86400 * 7
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    now = int(time.time())
    all_data: list[dict[str, Any]] = []
    solutions = await db.get_solutions_by_time(now - interval[type])
    address_list = list(set(map(lambda x: x['address'], solutions)))
    address_count = len(address_list)
    if offset < 0 or offset > address_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    for address in address_list:
        cur_solution = [solution for solution in solutions if solution["address"] == address]
        address_type = await get_address_type(db, address)
        all_data.append({
            "address": address,
            "address_type": address_type,
            "count": len(cur_solution),
            "power": float(sum(solution["pre_proof_target"] for solution in cur_solution) / interval[type]),
        })
    leaderboard_data = sorted(all_data, key=lambda e: e['power'], reverse=True)
    if offset + limit > len(leaderboard_data):
        data = leaderboard_data[offset:]
    else:
        data = leaderboard_data[offset:offset + limit]

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
    programs = await db.get_programs_by_address(address)
    transitions = await db.get_address_recent_transitions(address)
    try:
        address_key = LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads(address),
            )
        )
    except:
        raise HTTPException(status_code=404, detail="Address error")
    address_key_bytes = address_key.dump()
    account_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "account", address_key_bytes)
    bonded_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "bonded", address_key_bytes)
    unbonded_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "unbonding", address_key_bytes)
    committee_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "committee", address_key_bytes)
    public_balance_bytes = await db.get_mapping_value("credits.aleo", "account", account_key_id)
    bond_state_bytes = await db.get_mapping_value("credits.aleo", "bonded", bonded_key_id)
    unbond_state_bytes = await db.get_mapping_value("credits.aleo", "unbonding", unbonded_key_id)
    committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
    stake_reward = await db.get_address_stake_reward(address)
    delegate_reward = await db.get_address_delegate_reward(address)
    transfer_in = await db.get_address_transfer_in(address)
    transfer_out = await db.get_address_transfer_out(address)
    fee = await db.get_address_total_fee(address)
    address_info = await db.get_address_info(address)

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
        and address_info is None
    ):
        return JSONResponse({})
    now = int(time.time())
    if len(solutions) > 0:
        solution_count = await db.get_solution_count_by_address(address)
        total_rewards, total_incentive = await db.get_leaderboard_rewards_by_address(address)
        solutions_15min = await db.get_solutions_by_address_and_time(address, now - 900)
        reward_15min = sum(solution["reward"] for solution in solutions_15min)
        solutions_1h = await db.get_solutions_by_address_and_time(address, now - 3600)
        reward_1h = sum(solution["reward"] for solution in solutions_1h)
        solutions_1d = await db.get_solutions_by_address_and_time(address, now - 86400)
        reward_1d = sum(solution["reward"] for solution in solutions_1d)
        solutions_7d = await db.get_solutions_by_address_and_time(address, now - 86400 * 7)
        reward_7d = sum(solution["reward"] for solution in solutions_7d)
        speed, interval = await db.get_address_speed(address)
    else:
        solution_count = 0
        reward_15min = 0
        reward_1h = 0
        reward_1d = 0
        reward_7d = 0
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
            "commitment": ""
        })
    deploy_programs: list[dict[str, Any]] = []
    for program in programs:
        deploy_info = await db.get_deploy_info_by_program_id(program)
        if deploy_info is None:
            raise HTTPException(status_code=550, detail="Deploy info not found")
        block = await db.get_block_by_program_id(program)
        if block is None:
            raise HTTPException(status_code=550, detail="Deploy block not found")
        transaction: DeployTransaction | None = None
        for ct in block.transactions:
            if isinstance(ct, AcceptedDeploy):
                tx = ct.transaction
                if isinstance(tx, DeployTransaction) and str(tx.deployment.program.id) == program:
                    transaction = tx
                    break
        if transaction is None:
            raise HTTPException(status_code=550, detail="Deploy transaction not found")
        base_fee, priority_fee = transaction.fee.amount
        deploy_programs.append({
            "program_id": program,
            "height": deploy_info["height"],
            "timestamp": deploy_info["timestamp"],
            "deploy_fee": base_fee+priority_fee,
            "times_called": int(await db.get_program_called_times(program)),
            "transaction_id": deploy_info["transaction_id"],
            "called_trend": await db.get_program_calls_trend(program)
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
    if delegate_reward is None:
        delegate_reward = 0
    if transfer_in is None:
        transfer_in = 0
    if transfer_out is None:
        transfer_out = 0
    if fee is None:
        fee = 0
    address_type = ""
    total_stake = 0
    if committee_state:
        address_type = "Validator"
        total_stake = await db.get_total_stake()
    elif solution_count > 0:
        address_type = "Prover"
    elif program_count > 0:
        address_type = "Developer"
    if address_info is None:
        functions: list[str] = []
        favorites: dict[str, Any] = {}
        address_info = {
            "execution_transactions": 0,
            "fee_transactions": 0,
            "functions": functions,
            "favorites": favorites
        }

    sync_info = await out_of_sync_check(db)
    network_1hour_speed = await db.get_network_speed(3600)
    network_1hour_reward = await db.get_network_reward(3600)
    address_1hour_reward = await db.get_address_reward(address, 3600)
    uiaddress = await UIAddress(address).resolve(db)
    ctx = {
        "address": uiaddress.__dict__,
        "address_trunc": address[:14] + "..." + address[-6:],
        "address_type": address_type,
        "total_rewards": int(total_rewards),
        "15min_rewards": int(reward_15min),
        "1h_rewards": int(reward_1h),
        "1d_rewards": int(reward_1d),
        "7d_rewards": int(reward_7d),
        "total_incentive": int(total_incentive),
        "total_solutions": solution_count,
        "total_programs": program_count,
        "total_execution_transactions": address_info["execution_transactions"],
        "total_fee_transactions": address_info["fee_transactions"],
        "function_names": address_info["functions"],
        "favorites": address_info["favorites"],
        "speed": float(speed),
        "timespan": interval_text[interval],
        "public_credits": public_balance,
        "bond_state": bond_state,
        "unbond_state": unbond_state,
        "committee_state": committee_state,
        "stake_reward": stake_reward,
        "delegate_reward": delegate_reward,
        "total_stake": total_stake,
        "transfer_in": transfer_in,
        "transfer_out": transfer_out,
        "fee": fee,
        "address_1hour_reward": int(address_1hour_reward),
        "network": {
            "network_1hour_speed": float(network_1hour_speed),
            "network_1hour_reward": int(network_1hour_reward)

        },
        "solutions": recent_solutions,
        "programs": deploy_programs,
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
    address_info = await db.get_address_info(address)
    if address_info is None:
        raise HTTPException(status_code=400, detail="Invalid page")
    address_transaction_count = address_info["execution_transactions"]+address_info["fee_transactions"]
    if offset < 0 or offset > address_transaction_count:
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
        state = "Pending"
        if transition_data["type"]:
            if transition_data["type"].startswith("Accepted"):
                state = "Accepted"
            elif transition_data["type"].startswith("Rejected"):
                state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"] if transition_data["height"] is not None else "Pending",
            "timestamp": transition_data["timestamp"] if transition_data["timestamp"] else transition_data["first_seen"],
            "transaction_id": transition_data["transaction_id"],
            "from": (await UIAddress(from_address).resolve(db)).__dict__ if from_address else from_address,
            "to": (await UIAddress(to_address).resolve(db)).__dict__ if to_address else to_address,
            "credit": credit,
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })

    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "total_execution_transactions": address_info["execution_transactions"],
        "total_fee_transactions": address_info["fee_transactions"],
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
        state = "Pending"
        if transition_data["type"]:
            if transition_data["type"].startswith("Accepted"):
                state = "Accepted"
            elif transition_data["type"].startswith("Rejected"):
                state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"] if transition_data["height"] is not None else "Pending",
            "timestamp": transition_data["timestamp"] if transition_data["timestamp"] else transition_data["first_seen"],
            "transaction_id": transition_data["transaction_id"],
            "from": (await UIAddress(from_address).resolve(db)).__dict__ if from_address else from_address,
            "to": (await UIAddress(to_address).resolve(db)).__dict__ if to_address else to_address,
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


async def address_bonds_transaction_route(request: Request):
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
    bonds_count = await db.get_bond_transition_count_by_address(address)
    if offset < 0 or offset > bonds_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    bond_transitions = await db.get_bond_transition_by_address(address, offset, offset + limit)
    data: list[dict[str, Any]] = []
    for transition_data in bond_transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            raise HTTPException(status_code=550, detail="Transition not found")
        validator = None
        if transition.function_name == "bond_public":
            output = cast(FutureTransitionOutput, transition.outputs[0])
            future = cast(Future, output.future.value)
            validator = str(cast(LiteralPlaintext, cast(PlaintextArgument, cast(PlaintextArgument, future.arguments[1]).plaintext)).literal.primitive)
        if transition.function_name == "unbond_public":
            validator = await db.get_unbond_validator_by_address(address, transition_data["height"])
        state = "Pending"
        if transition_data["type"]:
            if transition_data["type"].startswith("Accepted"):
                state = "Accepted"
            elif transition_data["type"].startswith("Rejected"):
                state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"] if transition_data["height"] is not None else "Pending",
            "timestamp": transition_data["timestamp"] if transition_data["timestamp"] else transition_data["first_seen"],
            "transaction_id": transition_data["transaction_id"],
            "validator": (await UIAddress(validator).resolve(db)).__dict__ if validator else validator,
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })

    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "bonds_count": bonds_count,
        "transactions": data,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)


async def address_transfer_transaction_route(request: Request):
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
    transfer_count = await db.get_transition_count_by_address_program_id_function(address, "credits.aleo", "transfer_public")
    if offset < 0 or offset > transfer_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    transfer_transitions = await db.get_transition_by_address_program_id_function(address, "credits.aleo", "transfer_public", offset, offset + limit)
    data: list[dict[str, Any]] = []
    for transition_data in transfer_transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            raise HTTPException(status_code=550, detail="Transition not found")
        output = cast(FutureTransitionOutput, transition.outputs[0])
        future = cast(Future, output.future.value)
        transfer_from = str(Database.get_primitive_from_argument_unchecked(future.arguments[0]))
        transfer_to = str(Database.get_primitive_from_argument_unchecked(future.arguments[1]))
        amount = int(cast(int, Database.get_primitive_from_argument_unchecked(future.arguments[2])))
        state = "Pending"
        if transition_data["type"]:
            if transition_data["type"].startswith("Accepted"):
                state = "Accepted"
            elif transition_data["type"].startswith("Rejected"):
                state = "Rejected"
        data.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"] if transition_data["height"] is not None else "Pending",
            "timestamp": transition_data["timestamp"] if transition_data["timestamp"] else transition_data["first_seen"],
            "transaction_id": transition_data["transaction_id"],
            "transfer_from": (await UIAddress(transfer_from).resolve(db)).__dict__ if transfer_from else transfer_from,
            "transfer_to": (await UIAddress(transfer_to).resolve(db)).__dict__ if transfer_to else transfer_to,
            "credits": amount,
            "state": state,
            "program_id": str(transition.program_id),
            "function_name": str(transition.function_name),
        })

    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "transfer_count": transfer_count,
        "transactions": data,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)


async def address_trending_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    type = request.query_params.get("type")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    if type is None:
        raise HTTPException(status_code=400, detail="Missing trending type")
    current_data = int(time.time())
    if type == "1d":
        today_zero_time = current_data - int(time.time() - time.timezone) % 86400
        previous_timestamp = today_zero_time - 86400 * 30
        trending_time = today_zero_time
    elif type == "1h":
        now_datetime_hour = current_data - (current_data % 3600)
        previous_timestamp = now_datetime_hour - 86400
        trending_time = now_datetime_hour
    else:
        raise HTTPException(status_code=400, detail="Error trending type")
    solutions = await db.get_solutions_by_address_and_time(address, previous_timestamp)
    counts_data: list[dict[str, Any]] = []
    power_data: list[dict[str, Any]] = []
    speed_data: list[dict[str, Any]] = []
    if len(solutions) > 0:
        cur_solution = [solution for solution in solutions if solution["timestamp"] >= trending_time]
        if type == "1d":
            for _ in range(1, 30):
                counts_data.append({
                    "timestamp": trending_time,
                    "count": len(cur_solution)
                })
                power_data.append({
                    "timestamp": trending_time,
                    "power": sum(solution["reward"] for solution in cur_solution)
                })
                speed_data.append({
                    "timestamp": trending_time,
                    "speed": float(sum(solution["pre_proof_target"] for solution in cur_solution) / 86400)
                })
                cur_solution = [solution for solution in solutions if
                                trending_time > solution["timestamp"] >= trending_time - 86400 * 1]
                trending_time = trending_time - 86400 * 1
        elif type == "1h":
            for _ in range(1, 24):
                counts_data.append({
                    "timestamp": trending_time,
                    "count": len(cur_solution)
                })
                power_data.append({
                    "timestamp": trending_time,
                    "power": sum(solution["reward"] for solution in cur_solution)
                })
                speed_data.append({
                    "timestamp": trending_time,
                    "speed": float(sum(solution["pre_proof_target"] for solution in cur_solution) / 3600)
                })
                cur_solution = [solution for solution in solutions if
                                trending_time > solution["timestamp"] >= trending_time - 3600 * 1]
                trending_time = trending_time - 3600 * 1
    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "counts_data": counts_data,
        "power_data": power_data,
        "speed_data": speed_data,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)


async def baseline_trending_route(request: Request):
    db: Database = request.app.state.db
    type = request.query_params.get("type")
    if type is None:
        raise HTTPException(status_code=400, detail="Missing trending type")
    current_data = int(time.time())
    if type == "1d":
        today_zero_time = current_data - int(time.time() - time.timezone) % 86400
        previous_timestamp = today_zero_time - 86400 * 30
        trending_time = today_zero_time
    elif type == "1h":
        now_datetime_hour = current_data - (current_data % 3600)
        previous_timestamp = now_datetime_hour - 86400
        trending_time = now_datetime_hour
    else:
        raise HTTPException(status_code=400, detail="Error trending type")
    solutions = await db.get_solutions_by_time(previous_timestamp)
    counts_data: list[dict[str, Any]] = []
    power_data: list[dict[str, Any]] = []
    speed_data: list[dict[str, Any]] = []
    if len(solutions) > 0:
        cur_solution = [solution for solution in solutions if solution["timestamp"] >= trending_time]
        if type == "1d":
            for _ in range(1, 30):
                counts_data.append({
                    "timestamp": trending_time,
                    "count": len(cur_solution)
                })
                power_data.append({
                    "timestamp": trending_time,
                    "power": sum(solution["reward"] for solution in cur_solution)
                })
                speed_data.append({
                    "timestamp": trending_time,
                    "speed": float(sum(solution["pre_proof_target"] for solution in cur_solution) / 86400)
                })
                cur_solution = [solution for solution in solutions if
                                trending_time > solution["timestamp"] >= trending_time - 86400 * 1]
                trending_time = trending_time - 86400 * 1
        elif type == "1h":
            for _ in range(1, 24):
                counts_data.append({
                    "timestamp": trending_time,
                    "count": len(cur_solution)
                })
                power_data.append({
                    "timestamp": trending_time,
                    "power": sum(solution["reward"] for solution in cur_solution)
                })
                speed_data.append({
                    "timestamp": trending_time,
                    "speed": float(sum(solution["pre_proof_target"] for solution in cur_solution) / 3600)
                })
                cur_solution = [solution for solution in solutions if
                                trending_time > solution["timestamp"] >= trending_time - 3600 * 1]
                trending_time = trending_time - 3600 * 1
    sync_info = await out_of_sync_check(db)
    ctx = {
        "counts_data": counts_data,
        "power_data": power_data,
        "speed_data": speed_data,
        "sync_info": sync_info,
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
    trending_time = today_zero_time

    validator_trend = await db.get_validator_trend(address, previous_timestamp)
    stake_data: list[dict[str, Any]] = []
    profit_data: list[dict[str, Any]] = []
    if len(validator_trend) > 0:
        cur_trend = [trend for trend in validator_trend if trend["timestamp"] >= trending_time]
        for _ in range(1, 15):
            stake_data.append({
                "timestamp": trending_time,
                "value": int(cur_trend[0]["committee_stake"]) if cur_trend else 0
            })
            profit_data.append({
                "timestamp": trending_time,
                "value": int(sum(trend["stake_reward"]+trend["delegate_reward"]  for trend in cur_trend))

            })
            cur_trend = [trend for trend in validator_trend if
                            trending_time > trend["timestamp"] >= trending_time - 86400 * 1]
            trending_time = trending_time - 86400 * 1
    sync_info = await out_of_sync_check(db)
    ctx = {
        "address": address,
        "address_trunc": address[:14] + "..." + address[-6:],
        "stake_data": stake_data,
        "profit_data": profit_data,
        "sync_info": sync_info,
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
    sync_info = await out_of_sync_check(db)
    ctx = {
        "delegators": delegators,
        "delegator_count": delegator_count,
        "sync_info": sync_info,
    }
    return JSONResponse(ctx)


async def estimate_fee_route(request: Request):
    db: Database = request.app.state.db
    function = request.query_params.get("function")
    program_id = request.query_params.get("program")
    if function is None:
        raise HTTPException(status_code=400, detail="Missing function")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    transactions = await db.get_transaction_by_function(function, program_id)
    data: list[dict[str, Any]] = []
    gas_list: list[int] = []
    for transaction in transactions:
        transaction_id = transaction["transaction_id"]
        confirmed_transaction = await db.get_confirmed_transaction(transaction_id)
        storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)
        gas_fee = storage_cost + namespace_cost + sum(finalize_costs) + priority_fee + burnt
        gas_list.append(gas_fee)
        data.append({
            "height": transaction["height"],
            "transaction_id": transaction_id,
            "gas_fee": gas_fee,
        })
    ctx = {
        "function": function,
        "estimate_fee": max(set(gas_list), key=gas_list.count),
        "fees": data
    }
    return JSONResponse(ctx)


async def validator_apr_route(request: Request):
    db: Database = request.app.state.db
    address = request.query_params.get("a")
    if address is None:
        raise HTTPException(status_code=400, detail="Missing address")
    now = int(time.time())
    validator_trend = await db.get_validator_trend(address, now - 86400)
    dpr = sum((trend["stake_reward"]+trend["delegate_reward"])/trend["committee_stake"] for trend in validator_trend)
    ctx = {
        "daily_percentage_rate": float(dpr),
        "validator": address,
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


async def address_favorite_route(request: Request):
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
    favorites = await db.get_favorite_by_address(address)
    if offset < 0 or offset > len(favorites):
        raise HTTPException(status_code=400, detail="Invalid page")
    favorite_data: list[dict[str, Any]] = []
    for favorite in favorites:
        favorite_data.append({
            "address": favorite,
            "lable": favorites[favorite]
        })
    if offset + limit > len(favorite_data):
        data = favorite_data[offset:]
    else:
        data = favorite_data[offset:offset + limit]
    ctx = {
        "address": address,
        "favorites": data,
        "total_favorites": len(favorites)
    }
    return JSONResponse(ctx)


async def favorites_update_route(request: Request):
    db: Database = request.app.state.db
    json = await request.json()
    address = json.get("address")
    favorite = json.get("favorite")
    label = json.get("label")
    if not address:
        return JSONResponse({"error": "Missing address"}, status_code=400)
    if not favorite:
        return JSONResponse({"error": "Missing favorite address"}, status_code=400)
    if label is None:
        label = ""
    favorites = await db.update_address_favorite(address, favorite, label)
    ctx = {
        "address": address,
        "favorites": favorites
    }
    return JSONResponse(ctx)