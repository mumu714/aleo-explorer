from typing import Any
from db import Block
from decimal import Decimal
from aleo_types import BlockRewardRatify, PuzzleRewardRatify, GenesisRatify, QuorumAuthority, BeaconAuthority

def format_block(block: Block):
    authority_type = ""
    if isinstance(block.authority, QuorumAuthority):
        authority_type = "Quorum"
    elif isinstance(block.authority, BeaconAuthority):
        authority_type = "Beacon"
    rs: list[dict[str, Any]] = []
    for ratification in block.ratifications:
        if isinstance(ratification, BlockRewardRatify):
            rs.append({"type": "block_reward", "amount": ratification.amount})
        elif isinstance(ratification, PuzzleRewardRatify):
            rs.append({"type": "puzzle_reward", "amount": ratification.amount})
    return {
            "block_hash": str(block.block_hash),
            "previous_hash": str(block.previous_hash),
            "header": {
                 "previous_state_root": str(block.header.previous_state_root),
                 "transactions_root": str(block.header.transactions_root),
                 "finalize_root": str(block.header.finalize_root),
                 "ratifications_root": str(block.header.ratifications_root),
                 "solutions_root": str(block.header.solutions_root),
                 "subdag_root": str(block.header.subdag_root),
                 "metadata": block.header.metadata.__dict__
            },
            "authority_type": authority_type,
            "ratifications": rs
        }

def format_number(data: dict[str, Any]) -> dict[str, Any]:
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = str(int(value))
    return data

def format_aleo_credit(mc: int | Decimal):
    if mc == "-":
        return "-"
    return int(Decimal(mc) / 1_000_000)
