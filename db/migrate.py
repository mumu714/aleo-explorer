from __future__ import annotations

from typing import Awaitable

import psycopg
import psycopg.sql
from psycopg.rows import DictRow
from redis.asyncio.client import Redis

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .block import DatabaseBlock

class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[DictRow], Redis[str]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_block_validator_index),
            (2, self.migrate_2_add_epcoh_table),
            (3, self.migrate_3_add_address_solution_count)
        ]
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    for migrated_id, method in migrations:
                        await cur.execute("SELECT COUNT(*) FROM _migration WHERE migrated_id = %s", (migrated_id,))
                        res = await cur.fetchone()
                        if res is None or res['count'] == 0:
                            print(f"DB migrating {migrated_id}")
                            async with conn.transaction():
                                await method(conn, self.redis)
                                await cur.execute("INSERT INTO _migration (migrated_id) VALUES (%s)", (migrated_id,))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def migrate_1_add_block_validator_index(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("CREATE INDEX block_validator_block_id_index ON block_validator (block_id)")

    @staticmethod
    async def migrate_2_add_epcoh_table(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute("SELECT height FROM block ORDER BY height DESC LIMIT 1")
            result = await cur.fetchone()
            if result is None:
                return
            last_height = result['height']
            epoch_num = 0
            while epoch_num < last_height // 360:
                epoch_start_height = epoch_num * 360
                epoch_end_height = (epoch_num + 1) * 360 - 1

                await cur.execute("SELECT previous_hash, timestamp FROM block WHERE height = %s", (epoch_start_height,))
                result = await cur.fetchone()
                if result is None: raise RuntimeError("no blocks in database")
                epoch_start_timestamp = result['timestamp']
                epoch_hash = result['previous_hash']

                await cur.execute("SELECT timestamp FROM block WHERE height = %s", (epoch_end_height,))
                result = await cur.fetchone()
                if result is None: raise RuntimeError("no blocks in database")
                epoch_end_timestamp = result['timestamp']

                await cur.execute(
                    "INSERT INTO epoch (epoch_num, start_timestamp, end_timestamp, epoch_hash) "
                    "VALUES (%s, %s, %s, %s) ",
                    (epoch_num, epoch_start_timestamp, epoch_end_timestamp, epoch_hash)
                )
                epoch_num += 1

    @staticmethod
    async def migrate_3_add_address_solution_count(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT address, COUNT(DISTINCT solution_id) AS solution_count FROM solution GROUP BY address"
            )
            res = await cur.fetchall()
            for r in res:
                await cur.execute(
                    "INSERT INTO address (address, solution_count) VALUES (%s, %s) "
                    "ON CONFLICT (address) DO UPDATE SET solution_count = %s",
                    (r["address"], r["solution_count"], r["solution_count"])
                )