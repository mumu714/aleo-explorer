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
            (1, self.migrate_1_add_address_transition_type)
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

    async def migrate_1_add_address_transition_type(self, conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT ts.transition_id, tx.transaction_id FROM address_transition at "
                "JOIN transition ts ON at.transition_id = ts.id "  
                "JOIN transaction tx ON tx.id = ts.transaction_id "
                "JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id"
            )
            ats = await cur.fetchall()
            for at in ats:
                confirmed_transaction = await cast(DatabaseBlock, self).get_confirmed_transaction(at["transaction_id"])
                if confirmed_transaction is not None:
                    if isinstance(confirmed_transaction, AcceptedDeploy):
                        await cur.execute(
                            "UPDATE address_transition at SET type = %s "
                            "FROM transition ts JOIN transaction tx ON tx.id = ts.transaction_id "
                            "WHERE at.transition_id = ts.id AND tx.transaction_id = %s",
                            ("Accepted", at["transaction_id"])
                        )
                    elif isinstance(confirmed_transaction, AcceptedExecute):
                        await cur.execute(
                            "UPDATE address_transition at SET type = %s "
                            "FROM transition ts JOIN transaction tx ON tx.id = ts.transaction_id "
                            "WHERE at.transition_id = ts.id AND tx.transaction_id = %s",
                            ("Accepted", at["transaction_id"])
                        )
                    elif isinstance(confirmed_transaction, RejectedExecute):
                        tx = confirmed_transaction.transaction
                        fee = cast(Fee, tx.fee)
                        await cur.execute(
                            "UPDATE address_transition at SET type = %s "
                            "FROM transition ts JOIN transaction tx ON tx.id = ts.transaction_id "
                            "WHERE at.transition_id = ts.id AND tx.transaction_id = %s AND ts.transition_id = %s",
                            ("Accepted", at["transaction_id"], str(fee.transition.id))
                        )
                        rejected = confirmed_transaction.rejected
                        if not isinstance(rejected, RejectedExecution):
                            raise ValueError("expected Rejected Execution transaction")
                        for ts in rejected.execution.transitions:
                            await cur.execute(
                                "UPDATE address_transition at SET type = %s "
                                "FROM transition ts JOIN transaction tx ON tx.id = ts.transaction_id "
                                "WHERE at.transition_id = ts.id AND tx.transaction_id = %s AND ts.transition_id = %s",
                                ("Rejected", at["transaction_id"], str(ts.id))
                            )
                    elif isinstance(confirmed_transaction, RejectedDeploy):
                            tx = confirmed_transaction.transaction
                            fee = cast(Fee, tx.fee)
                            await cur.execute(
                                "UPDATE address_transition at SET type = %s "
                                "FROM transition ts JOIN transaction tx ON tx.id = ts.transaction_id "
                                "WHERE at.transition_id = ts.id AND tx.transaction_id = %s AND ts.transition_id = %s",
                                ("Accepted", at["transaction_id"], str(fee.transition.id))
                            )
            
            await cur.execute("SELECT * FROM address_transition WHERE type = 'Rejected'")
            atms = await cur.fetchall()
            for atm in atms:
                await cur.execute(
                    "SELECT COUNT(DISTINCT transition_id) FROM address_transition WHERE address = %s "
                    "AND type = 'Rejected' AND program_id = %s AND function_name = %s", 
                    (atm["address"], atm["program_id"], atm["function_name"])
                )
                res = await cur.fetchone()
                if res is not None:
                    await cur.execute(
                        "UPDATE address_transition_summary at SET rejected_transition_count = %s "
                        "WHERE address = %s AND program_id = %s AND function_name = %s", 
                        (res["count"], atm["address"], atm["program_id"], atm["function_name"])
                    )