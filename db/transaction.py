from __future__ import annotations

from collections import defaultdict

import psycopg
import psycopg.sql

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseTransaction(DatabaseBase):
    
    async def get_transaction_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM transaction "
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transactions(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT t.transaction_id, t.confimed_transaction_id, b.height, ct.type, b.timestamp "
                        "FROM block b "
                        "JOIN confirmed_transaction ct ON b.id = ct.block_id "
                        "JOIN transaction t ON ct.id = t.confimed_transaction_id "
                        "ORDER BY ct.id DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM transition "
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transitions(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ts.transition_id, ts.program_id, function_name, ct.type "
                        "FROM transition ts "
                        "JOIN transaction_execute te on te.id = ts.transaction_execute_id "
                        "JOIN transaction t on te.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_count_by_address_and_function(self, address: str, function: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if function == "fee_public":
                        await cur.execute(
                            "SELECT COUNT(DISTINCT at.transition_id) FROM address_transition at "
                            "JOIN transition t on at.transition_id = t.id "
                            "JOIN fee on fee.id = t.fee_id "
                            "WHERE at.address = %s AND t.function_name = %s",(address,function,)
                        )
                    else:
                        await cur.execute(
                            "SELECT COUNT(DISTINCT at.transition_id) FROM address_transition at "
                            "JOIN transition t on at.transition_id = t.id "
                            "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                            "WHERE at.address = %s AND t.function_name = %s",(address,function,)
                        )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_bond_transition_count_by_address(self, address: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(DISTINCT at.transition_id) FROM address_transition at "
                        "JOIN transition t on at.transition_id = t.id "
                        "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                        "WHERE at.address = %s AND t.function_name = ANY(%s::text[])",
                        (address,["bond_public", "unbond_public", "claim_unbond_public"],)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_by_address(self, address: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT t.transition_id, b.height, b.timestamp, t2.transaction_id, ct.type FROM address_transition at "
                        "JOIN transition t on at.transition_id = t.id "
                        "LEFT JOIN transaction_execute te on te.id = t.transaction_execute_id "
                        "LEFT JOIN fee on fee.id = t.fee_id "
                        "LEFT JOIN transaction t2 on t2.id = te.transaction_id OR t2.id = fee.transaction_id "
                        "JOIN confirmed_transaction ct on ct.id = t2.confimed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE at.address = %s ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (address, end - start, start)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise
    
    async def get_transition_by_address_and_function(self, address: str, function: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if function == "fee_public":
                        await cur.execute(
                            "SELECT DISTINCT t.transition_id, b.height, b.timestamp, t2.transaction_id, ct.type FROM address_transition at "
                            "JOIN transition t on at.transition_id = t.id "
                            "JOIN fee on fee.id = t.fee_id "
                            "JOIN transaction t2 on t2.id = fee.transaction_id "
                            "JOIN confirmed_transaction ct on ct.id = t2.confimed_transaction_id "
                            "JOIN block b on b.id = ct.block_id "
                            "WHERE at.address = %s AND t.function_name = %s ORDER BY b.height DESC "
                            "LIMIT %s OFFSET %s",
                            (address, function, end - start, start)
                        )
                    else:
                        await cur.execute(
                            "SELECT DISTINCT t.transition_id, b.height, b.timestamp, t2.transaction_id, ct.type FROM address_transition at "
                            "JOIN transition t on at.transition_id = t.id "
                            "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                            "JOIN transaction t2 on t2.id = te.transaction_id "
                            "JOIN confirmed_transaction ct on ct.id = t2.confimed_transaction_id "
                            "JOIN block b on b.id = ct.block_id "
                            "WHERE at.address = %s AND t.function_name = %s ORDER BY b.height DESC "
                            "LIMIT %s OFFSET %s",
                            (address, function, end - start, start)
                        )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_bond_transition_by_address(self, address: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT t.transition_id, b.height, b.timestamp, t2.transaction_id, ct.type FROM address_transition at "
                        "JOIN transition t on at.transition_id = t.id "
                        "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                        "JOIN transaction t2 on t2.id = te.transaction_id "
                        "JOIN confirmed_transaction ct on ct.id = t2.confimed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE at.address = %s AND t.function_name = ANY(%s::text[]) ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (address, ["bond_public", "unbond_public", "claim_unbond_public"], end - start, start)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise