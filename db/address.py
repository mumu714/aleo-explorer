
from __future__ import annotations

import time

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseAddress(DatabaseBase):

    async def get_puzzle_reward_by_address(self, address: str) -> int:
        data = await self.redis.hget("address_puzzle_reward", address)
        if data is None:
            return 0
        return int(data)
    
    async def get_puzzle_reward_all(self):
        data = await self.redis.hgetall("address_puzzle_reward")
        return len(data), data

    async def get_recent_solutions_by_address(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, s.counter, s.target, s.solution_id, reward, ps.target_sum "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE s.address = %s "
                        "ORDER BY ps.id DESC "
                        "LIMIT 10",
                        (address,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_count_by_address(self, address: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM solution WHERE address = %s", (address,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_address(self, address: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, s.counter, s.target, s.solution_id, reward, ps.target_sum, s.commitment "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE s.address = %s "
                        "ORDER BY ps.id DESC "
                        "LIMIT %s OFFSET %s",
                        (address, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_count_by_height(self, height: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM solution s "
                        "JOIN puzzle_solution ps on ps.id = s.puzzle_solution_id "
                        "JOIN block b on b.id = ps.block_id "
                        "WHERE b.height = %s", (height,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_total_target_by_height(self, height: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT SUM(s.target) as total_target FROM solution s "
                        "JOIN puzzle_solution ps on ps.id = s.puzzle_solution_id "
                        "JOIN block b on b.id = ps.block_id "
                        "WHERE b.height = %s", (height,)
                    )
                    res = await cur.fetchone()
                    if res is None or res["total_target"] is None:
                        return 0
                    return res["total_target"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_height(self, height: int, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, s.counter, s.target, reward, s.solution_id "
                        "FROM solution s "
                        "JOIN puzzle_solution ps on s.puzzle_solution_id = ps.id "
                        "JOIN block b on ps.block_id = b.id "
                        "WHERE b.height = %s "
                        "ORDER BY target DESC "
                        "LIMIT %s OFFSET %s",
                        (height, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solutions_by_time(self, timestamp: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, b.height, b.timestamp, reward FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE b.timestamp > %s "
                        "ORDER BY b.timestamp DESC ",
                        (timestamp, )
                    )
                    prover_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], prover_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    def transform(x: dict[str, Any]):
                        return {
                            "address": x["address"],
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "reward": x["reward"],
                            "pre_proof_target": ref_proof_target_dict[x["height"]-1]
                        }
                    return list(map(lambda x: transform(x), prover_solutions))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solutions_by_address_and_time(self, address: str, timestamp: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, reward FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE s.address = %s AND b.timestamp > %s "
                        "ORDER BY b.timestamp DESC ",
                        (address, timestamp)
                    )
                    prover_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], prover_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    def transform(x: dict[str, Any]):
                        return {
                            "height": x["height"],
                            "timestamp": x["timestamp"],
                            "reward": x["reward"],
                            "pre_proof_target": ref_proof_target_dict[x["height"]-1]
                        }
                    return list(map(lambda x: transform(x), prover_solutions))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_recent_transitions(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
WITH ats AS
    (SELECT DISTINCT transition_id
     FROM address_transition
     WHERE address = %s
     ORDER BY transition_id DESC
     LIMIT 10)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                ct.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction_execute te ON te.id = ts.transaction_execute_id
JOIN transaction tx ON tx.id = te.transaction_id
JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
JOIN block b ON b.id = ct.block_id
UNION
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp,
                tx.transaction_id, 
                ct.type
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN fee f ON f.id = ts.fee_id
JOIN transaction tx ON tx.id = f.transaction_id
JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
LIMIT 10
""",
                        (address,)
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

    async def get_address_stake_reward(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_stake_reward", address)
        if data is None:
            return None
        return int(data)

    async def get_address_delegate_reward(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_delegate_reward", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_in(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_in", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_out(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_out", address)
        if data is None:
            return None
        return int(data)

    async def get_address_total_fee(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_fee", address)
        if data is None:
            return None
        return int(data)

    async def get_address_speed(self, address: str) -> tuple[float, int]: # (speed, interval)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval_list = [900, 1800, 3600, 14400, 43200, 86400]
                now = int(time.time())
                try:
                    for interval in interval_list:
                        await cur.execute(
                            "SELECT b.height FROM solution s "
                            "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                            "JOIN block b ON ps.block_id = b.id "
                            "WHERE address = %s AND timestamp > %s",
                            (address, now - interval)
                        )
                        partial_solutions = await cur.fetchall()
                        if len(partial_solutions) < 10:
                            continue
                        heights = list(map(lambda x: x['height'], partial_solutions))
                        ref_heights = list(map(lambda x: x - 1, set(heights)))
                        await cur.execute(
                            "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                        )
                        ref_proof_targets = await cur.fetchall()
                        ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                        total_solutions = 0
                        for height in heights:
                            total_solutions += ref_proof_target_dict[height - 1]
                        return total_solutions / interval, interval
                    return 0, 0
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_speed(self, interval: int) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                now = int(time.time())
                try:
                    await cur.execute(
                        "SELECT b.height FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE timestamp > %s",
                        (now - interval,)
                    )
                    partial_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], partial_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    total_solutions = 0
                    for height in heights:
                        total_solutions += ref_proof_target_dict[height - 1]
                    return total_solutions / interval
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_puzzle_commitment(self, solution_id: str) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT height, block_hash, reward, address, target_sum, solution_id FROM solution s "
                        "JOIN puzzle_solution ps on s.puzzle_solution_id = ps.id "
                        "JOIN block b on b.id = ps.block_id "
                        "WHERE solution_id = %s",
                        (solution_id,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return row
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_credits_leaderboard(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                            "SELECT address, public_credits FROM address WHERE public_credits > 0 "
                            "ORDER BY public_credits DESC "
                            "LIMIT %s OFFSET %s",
                            (end - start, start)
                        )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_credits_leaderboard_size(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM address WHERE public_credits > 0")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_reward(self, address: str, interval: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                now = int(time.time())
                try:
                    await cur.execute(
                        "SELECT s.reward FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE address = %s AND timestamp > %s",
                        (address, now - interval)
                    )
                    address_rewards = await cur.fetchall()
                    reward = sum(list(map(lambda x: x['reward'] if x['reward'] else 0, address_rewards)))
                    return reward
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_info(self, address: str) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM address at WHERE at.address = %s", (address,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return {
                        'execution_transactions': row['execution_ts_num'],
                        'fee_transactions': row['fee_ts_num'],
                        'functions': row['functions'],
                        'favorites': row['favorite']
                    }
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_15min_speed(self, address: str) -> float: # (speed, interval)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval = 900
                now = int(time.time())
                try:
                    await cur.execute(
                        "SELECT b.height FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE address = %s AND timestamp > %s",
                        (address, now - interval)
                    )
                    prover_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], prover_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    total_solutions = 0
                    for height in heights:
                        total_solutions += ref_proof_target_dict[height - 1]
                    return total_solutions / interval
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_15min_top_miner(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM address_15min_hashrate "
                        "ORDER BY hashrate DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_hashrate(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM hashrate "
                        "ORDER BY timestamp DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_coinbase(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM coinbase ORDER BY height")
                    coinbase = await cur.fetchall()
                    return coinbase
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_reward(self, interval: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                now = int(time.time())
                try:
                    await cur.execute(
                        "SELECT coinbase_reward FROM block b  "
                        "WHERE timestamp > %s",
                        (now - interval,)
                    )
                    coinbase_rewards = await cur.fetchall()
                    reward = sum(list(map(lambda x: x['coinbase_reward'] if x['coinbase_reward'] else 0, coinbase_rewards)))
                    return reward  * 2 // 3
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_favorite_by_address(self, address: str) -> dict[str, Any]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT favorite FROM address WHERE address = %s", (address,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return {}
                    return res["favorite"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise
    async def get_total_solution_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM solution")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_average_solution_reward(self) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT AVG(reward) FROM solution"
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    if res["avg"] is None:
                        return 0
                    return res["avg"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_address_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(DISTINCT address) FROM solution "
                        "JOIN puzzle_solution ps ON solution.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200"
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_addresses(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, sum(s.reward) as reward FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200 "
                        "GROUP BY s.address "
                        "ORDER BY reward DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_total_reward(self) -> Decimal:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT sum(reward) FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200"
                    )
                    if (res := await cur.fetchone()) is None:
                        return Decimal(0)
                    return res["sum"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_id(self, solution_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, s.counter, s.target, s.reward, ps.target_sum, b.height, b.timestamp "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE s.solution_id = %s",
                        (solution_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise