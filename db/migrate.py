from __future__ import annotations

from typing import Awaitable, LiteralString

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
            (1, self.migrate_1_add_address_transition_type),
            (2, self.migration_2_add_output_record_sender_ciphertext),
            (3, self.migration_3_add_program_edition),
            (4, self.migration_4_add_program_checksum),
            (5, self.migration_5_add_dynamic_future_argument_type),
            (6, self.migration_6_add_dynamic_transition_types),
            (7, self.migration_7_rename_future_argument_plaintext),
            (8, self.migration_8_fix_dynamic_transition_type_fk_cascade),
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

    @staticmethod
    async def migration_2_add_output_record_sender_ciphertext(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute(cast(LiteralString, open("db/migrate_5.sql").read()))

    @staticmethod
    async def migration_3_add_program_edition(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("alter table program add edition integer default 0 not null")
        await conn.execute("alter table program drop constraint program_pk2")
        await conn.execute("alter table program add constraint program_pk2 unique (program_id, edition)")
    
    @staticmethod
    async def migration_4_add_program_checksum(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("alter table program add checksum bytea")

    @staticmethod
    async def migration_5_add_dynamic_future_argument_type(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("ALTER TYPE argument_type ADD VALUE 'DynamicFuture'")

    @staticmethod
    async def migration_6_add_dynamic_transition_types(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute(cast(LiteralString, open("db/migrate_11.sql").read()))

    @staticmethod
    async def migration_7_rename_future_argument_plaintext(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("ALTER TABLE future_argument RENAME COLUMN plaintext TO data")

    @staticmethod
    async def migration_8_fix_dynamic_transition_type_fk_cascade(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        # migrate_11.sql 建这几张 dynamic 表时漏了 ON DELETE CASCADE，导致回滚区块
        # （DELETE FROM block WHERE height > ...）级联到 transition_input/transition_output
        # 时被外键挡住，报 ForeignKeyViolation。这里把外键删掉重建成带 CASCADE 的版本。
        # 约束名由 PG 自动生成且会截断到 63 字节，所以从 pg_constraint 动态查，不写死。
        await conn.execute("""
DO $$
DECLARE
    t record;
    con_name text;
BEGIN
    FOR t IN
        SELECT * FROM (VALUES
            ('transition_input_dynamic_record', 'transition_input_id', 'transition_input'),
            ('transition_input_record_with_dynamic_id', 'transition_input_id', 'transition_input'),
            ('transition_input_external_record_with_dynamic_id', 'transition_input_id', 'transition_input'),
            ('transition_output_dynamic_record', 'transition_output_id', 'transition_output'),
            ('transition_output_record_with_dynamic_id', 'transition_output_id', 'transition_output'),
            ('transition_output_external_record_with_dynamic_id', 'transition_output_id', 'transition_output')
        ) AS v(child, col, parent)
    LOOP
        con_name := NULL;
        SELECT c.conname INTO con_name
        FROM pg_constraint c
        WHERE c.conrelid = t.child::regclass
          AND c.contype = 'f'
          AND c.confrelid = t.parent::regclass
          AND c.conkey = ARRAY[(SELECT a.attnum FROM pg_attribute a
                                WHERE a.attrelid = t.child::regclass AND a.attname = t.col)]
          AND c.confdeltype <> 'c';  -- 'c' = CASCADE，已经是 CASCADE 的跳过
        IF con_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', t.child, con_name);
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I(id) ON DELETE CASCADE',
                t.child, left(t.child || '_' || t.col || '_fk', 63), t.col, t.parent
            );
        END IF;
    END LOOP;
END $$;
""")
