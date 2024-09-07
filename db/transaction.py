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
                        "SELECT COUNT(*) FROM transaction WHERE confirmed_transaction_id IS not NULL"
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
                        "SELECT t.transaction_id, t.confirmed_transaction_id, b.height, ct.type, b.timestamp "
                        "FROM block b "
                        "JOIN confirmed_transaction ct ON b.id = ct.block_id "
                        "JOIN transaction t ON ct.id = t.confirmed_transaction_id "
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
                        "SELECT COUNT(*) FROM transition WHERE confirmed_transaction_id IS NOT NULL"
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
                        "SELECT b.height, b.timestamp, ts.transition_id, ts.program_id, ts.function_name, ct.type "
                        "FROM transition ts "
                        "JOIN confirmed_transaction ct on ts.confirmed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "ORDER BY height DESC "
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
                    await cur.execute(
                        "SELECT SUM(transition_count) AS count FROM address_transition_summary "
                        "WHERE address = %s AND function_name = %s ",(address,function,)
                    )
                    res = await cur.fetchone()
                    if res is None or res["count"] is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_count_by_address_program_id_function(self, address: str, program_id:str, function: str) -> dict[str, int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT transition_count, rejected_transition_count  FROM address_transition_summary "
                        "WHERE address = %s AND program_id = %s AND function_name = %s ",(address,program_id, function,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return {"transition_count": 0, "rejected_transition_count": 0}
                    return res
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_bond_transition_count_by_address(self, address: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT SUM(transition_count) AS count FROM address_transition_summary "
                        "WHERE address = %s AND function_name = ANY(%s::text[])",
                        (address, ["bond_public", "unbond_public", "claim_unbond_public"],)
                    )
                    res = await cur.fetchone()
                    if res is None or res["count"] is None:
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
                        """
WITH ats AS
    (SELECT DISTINCT transition_id, type 
     FROM address_transition
     WHERE address = %s
     ORDER BY transition_id DESC
     LIMIT %s OFFSET %s)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                tx.first_seen,
                ats.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction tx ON tx.id = ts.transaction_id
LEFT JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
LEFT JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
""",
                        (address, end - start, start)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"],
                            "first_seen": x["first_seen"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise
    
    async def get_transition_by_address_and_function(self, address: str, function: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
WITH ats AS
    (SELECT DISTINCT transition_id, type
     FROM address_transition at
     WHERE address = %s AND function_name = %s
     ORDER BY transition_id DESC
     LIMIT %s OFFSET %s)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                tx.first_seen,
                ats.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction tx ON tx.id = ts.transaction_id
LEFT JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
LEFT JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
""",
                            (address, function, end - start, start)
                        )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"],
                            "first_seen": x["first_seen"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_by_address_program_id_function(self, address: str, program_id: str, function: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
WITH ats AS
    (SELECT DISTINCT transition_id, type
     FROM address_transition at
     WHERE address = %s AND program_id = %s AND function_name = %s
     ORDER BY transition_id DESC
     LIMIT %s OFFSET %s)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                tx.first_seen,
                ats.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction tx ON tx.id = ts.transaction_id
LEFT JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
LEFT JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
""",
                            (address, program_id, function, end - start, start)
                        )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"],
                            "first_seen": x["first_seen"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transition_by_address_program_id_function_type(self, address: str, program_id: str, function: str, start: int, end: int, type: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
WITH ats AS
    (SELECT DISTINCT transition_id, type 
     FROM address_transition at
     WHERE address = %s AND program_id = %s AND function_name = %s AND type = %s 
     ORDER BY transition_id DESC
     LIMIT %s OFFSET %s)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                tx.first_seen,
                ats.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction tx ON tx.id = ts.transaction_id
LEFT JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
LEFT JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
""",
                            (address, program_id, function, type, end - start, start)
                        )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"],
                            "first_seen": x["first_seen"]
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
                        """
WITH ats AS
    (SELECT DISTINCT transition_id, type 
     FROM address_transition at
     WHERE address = %s AND function_name = ANY(%s::text[])
     ORDER BY transition_id DESC
     LIMIT %s OFFSET %s)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                tx.first_seen,
                ats.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction tx ON tx.id = ts.transaction_id
LEFT JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
LEFT JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
""",
                        (address, ["bond_public", "unbond_public", "claim_unbond_public"], end - start, start)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "transaction_id": x["transaction_id"],
                            "type": x["type"],
                            "first_seen": x["first_seen"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transaction_by_function(self, function: str, program_id: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT t2.transaction_id, b.height FROM transition t "
                        "JOIN transaction_execute te on te.id = t.transaction_execute_id "
                        "JOIN transaction t2 on t2.id = te.transaction_id "
                        "JOIN confirmed_transaction ct on ct.id = t2.confirmed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE t.function_name = %s AND t.program_id = %s ORDER BY b.height DESC LIMIT 10",
                        (function, program_id)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transaction_id": x["transaction_id"],
                            "height": x["height"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise
