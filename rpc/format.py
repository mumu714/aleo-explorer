from typing import Any
from db import Block
from decimal import Decimal

def format_block(block: Block):
    return {
            "block_hash": str(block.block_hash),
            "previous_hash": str(block.previous_hash),
            "header": {
                 "previous_state_root": str(block.header.previous_state_root),
                 "transactions_root": str(block.header.transactions_root),
                 "coinbase_accumulator_point": str(block.header.coinbase_accumulator_point),
                 "finalize_root": str(block.header.finalize_root),
                 "ratifications_root": str(block.header.ratifications_root),
                 "metadata": block.header.metadata.__dict__
            },
            "signature": str(block.signature),
            "ratifications": [{"address": str(i.address), "amount": i.amount} for i in block.ratifications]
        }

def format_number(block: dict[str, Any]) -> dict[str, Any]:
    for key, value in block.items():
        if isinstance(value, Decimal):
            block[key] = str(int(value))
    return block
