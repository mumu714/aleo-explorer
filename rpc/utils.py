import asyncio
import os
import time

import aiohttp

from db import Database
import aleo_explorer_rust
from aleo_types import *
from typing import cast


def get_relative_time(timestamp: int):
    now = time.time()
    delta = int(now - timestamp)
    if delta == 0:
        return "just now"
    elif delta == 1:
        return "1 second ago"
    elif delta < 60:
        return f"{int(delta)} seconds ago"
    delta = delta // 60
    if delta == 1:
        return "1 minute ago"
    elif delta < 60:
        return f"{int(delta)} minutes ago"
    delta = delta // 60
    if delta == 1:
        return "1 hour ago"
    return f"{int(delta)} hours ago"


async def get_remote_height(session: aiohttp.ClientSession, rpc_root: str) -> str:
    try:
        async with session.get(f"{rpc_root}/testnet/latest/height") as resp:
            if resp.status == 200:
                remote_height = await resp.text()
            else:
                remote_height = "?"
    except:
        remote_height = "?"
    return remote_height


async def out_of_sync_check(session: aiohttp.ClientSession, db: Database):
    last_timestamp, last_height = await asyncio.gather(
        db.get_latest_block_timestamp(),
        db.get_latest_height()
    )
    now = int(time.time())
    maintenance_info = os.environ.get("MAINTENANCE_INFO")
    out_of_sync = now - last_timestamp > 120
    node_height = None
    reference_height = None
    if out_of_sync:
        if rpc_root := os.environ.get("RPC_URL_ROOT"):
            node_height = await get_remote_height(session, rpc_root)
        if ref_rpc_root := os.environ.get("REF_RPC_URL_ROOT"):
            reference_height = await get_remote_height(session, ref_rpc_root)

    return {
        "out_of_sync": out_of_sync,
        "maintenance_info": maintenance_info,
        "explorer_height": last_height,
        "node_height": node_height,
        "reference_height": reference_height,
        "relative_time": get_relative_time(last_timestamp),
    }


async def function_signature(db: Database, program_id: str, function_name: str):
    data = await function_definition(db, program_id, function_name)
    if isinstance(data, str):
        return data
    inputs: list[str] = []
    for i in range(len(data["input"])):
        name = data["input"][i]
        mode = data["input_mode"][i]
        if mode == "private":
            inputs.append(name)
        else:
            inputs.append(f"{mode} {name}")
    outputs: list[str] = []
    for i in range(len(data["output"])):
        name = data["output"][i]
        mode = data["output_mode"][i]
        if mode == "private":
            outputs.append(name)
        else:
            outputs.append(f"{mode} {name}")
    finalizes = data["finalize"]
    result = f"{program_id}/{function_name}({', '.join(inputs)})"
    if len(outputs) == 1:
        result += f" -> {outputs[0]}"
    else:
        result += f" -> ({', '.join(outputs)})"
    if len(finalizes) != 0:
        result += f" finalize({', '.join(finalizes)})"
    return result

async def function_definition(db: Database, program_id: str, function_name: str):
    data = await db.get_function_definition(program_id, function_name)
    if data is None:
        return f"Unknown function {program_id}/{function_name}"
    return data

async def get_address_type(db: Database, address: str):
    address_key = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Address,
            primitive=Address.loads(address),
        )
    )
    address_key_bytes = address_key.dump()
    committee_key_id = aleo_explorer_rust.get_key_id("credits.aleo", "committee", address_key_bytes)
    committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
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
    address_type = ""
    solution_count = await db.get_solution_count_by_address(address)
    program_count = await db.get_program_count_by_address(address)
    if committee_state:
        address_type = "Validator"
    elif solution_count > 0:
        address_type = "Prover"
    elif program_count > 0:
        address_type = "Developer"
    return address_type
