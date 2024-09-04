from __future__ import annotations

import psycopg
import psycopg.sql

from aleo_types import *
from aleo_types.cached import cached_get_mapping_id
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from typing import cast


class DatabaseMapping(DatabaseBase):
    async def get_mapping_cache_with_cur(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                         mapping_name: str) -> dict[Field, Any]:
        if program_name == "credits.aleo" and mapping_name in ["committee", "bonded", "delegated"]:
            def transform(d: dict[str, Any]):
                return {
                    "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                    "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                }
            data = await self.redis.hgetall(f"{program_name}:{mapping_name}")
            return {Field.loads(k): transform(json.loads(v)) for k, v in data.items()}
        else:
            mapping_id = Field.loads(cached_get_mapping_id(program_name, mapping_name))
            try:
                await cur.execute(
                    "SELECT key_id, key, value FROM mapping_value mv "
                    "JOIN mapping m on mv.mapping_id = m.id "
                    "WHERE m.mapping_id = %s ",
                    (str(mapping_id),)
                )
                data = await cur.fetchall()
                def transform(d: dict[str, Any]):
                    return {
                        "key": Plaintext.load(BytesIO(d["key"])),
                        "value": Value.load(BytesIO(d["value"])),
                    }
                return {Field.loads(x["key_id"]): transform(x) for x in data}
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_mapping_cache(self, program_name: str, mapping_name: str) -> dict[Field, Any]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                return await self.get_mapping_cache_with_cur(cur, program_name, mapping_name)

    async def get_mapping_value(self, program_id: str, mapping: str, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded", "delegated"]:
                        conn = self.redis
                        data = await conn.hget(f"{program_id}:{mapping}", key_id)
                        if data is None:
                            return None
                        return bytes.fromhex(json.loads(data)["value"])
                    else:
                        await cur.execute(
                            "SELECT value FROM mapping_value mv "
                            "JOIN mapping m on mv.mapping_id = m.id "
                            "WHERE m.program_id = %s AND m.mapping = %s AND mv.key_id = %s",
                            (program_id, mapping, key_id)
                        )
                        res = await cur.fetchone()
                        if res is None:
                            return None
                        return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_size(self, program_id: str, mapping: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.program_id = %s AND m.mapping = %s",
                        (program_id, mapping)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_key_value(self, program_id: str, mapping: str, count: int, cursor: int = 0) -> tuple[dict[Field, Any], int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded", "delegated"]:
                        def transform(d: dict[str, Any]):
                            return {
                                "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                                "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                            }
                        conn = self.redis
                        data = await conn.hscan(f"{program_id}:{mapping}", cursor, count=count)
                        return {Field.loads(k): transform(json.loads(v)) for k, v in data[1].items()}, data[0]
                    else:
                        cursor_clause = psycopg.sql.SQL("AND mv.id < {} ").format(psycopg.sql.Literal(cursor)) if cursor > 0 else psycopg.sql.SQL("")
                        await cur.execute(
                            psycopg.sql.Composed([
                                psycopg.sql.SQL(
                                    "SELECT mv.id, key_id, key, value FROM mapping_value mv "
                                    "JOIN mapping m on mv.mapping_id = m.id "
                                    "WHERE m.program_id = %s AND m.mapping = %s "
                                ),
                                cursor_clause,
                                psycopg.sql.SQL(
                                    "ORDER BY mv.id DESC "
                                    "LIMIT %s"
                                )
                            ]),
                            (program_id, mapping, count)
                        )
                        data = await cur.fetchall()
                        def transform(d: dict[str, Any]):
                            return {
                                "key": Plaintext.load(BytesIO(d["key"])),
                                "value": Value.load(BytesIO(d["value"])),
                            }
                        cursor = data[-1]["id"] if len(data) > 0 else 0
                        return {Field.loads(x["key_id"]): transform(x) for x in data}, cursor
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_key_count(self, program_id: str, mapping: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded", "delegated"]:
                        conn = self.redis
                        return await conn.hlen(f"{program_id}:{mapping}")
                    else:
                        await cur.execute(
                            "SELECT COUNT(*) FROM mapping_value mv "
                            "JOIN mapping m on mv.mapping_id = m.id "
                            "WHERE m.program_id = %s AND m.mapping = %s",
                            (program_id, mapping)
                        )
                        if (res := await cur.fetchone()) is None:
                            return 0
                        return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_bonded_mapping(self) -> dict[Address, tuple[Address, u64]]:
        data = await self.redis.hgetall("credits.aleo:bonded")

        stakers: dict[Address, tuple[Address, u64]] = {}
        for d in data.values():
            d = json.loads(d)
            key = Plaintext.load(BytesIO(bytes.fromhex(d["key"])))
            if not isinstance(key, LiteralPlaintext):
                raise RuntimeError("invalid bonded key")
            if not isinstance(key.literal.primitive, Address):
                raise RuntimeError("invalid bonded key")
            value = Value.load(BytesIO(bytes.fromhex(d["value"])))
            if not isinstance(value, PlaintextValue):
                raise RuntimeError("invalid bonded value")
            plaintext = value.plaintext
            if not isinstance(plaintext, StructPlaintext):
                raise RuntimeError("invalid bonded value")
            validator = plaintext["validator"]
            if not isinstance(validator, LiteralPlaintext):
                raise RuntimeError("invalid bonded value")
            if not isinstance(validator.literal.primitive, Address):
                raise RuntimeError("invalid bonded value")
            amount = plaintext["microcredits"]
            if not isinstance(amount, LiteralPlaintext):
                raise RuntimeError("invalid bonded value")
            if not isinstance(amount.literal.primitive, u64):
                raise RuntimeError("invalid bonded value")
            stakers[key.literal.primitive] = validator.literal.primitive, amount.literal.primitive
        return stakers

    async def initialize_mapping(self, cur: psycopg.AsyncCursor[dict[str, Any]], mapping_id: str, program_id: str, mapping: str):
        try:
            await cur.execute(
                "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
                (mapping_id, program_id, mapping)
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def initialize_builtin_mapping(self, mapping_id: str, program_id: str, mapping: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (mapping_id, program_id, mapping)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def update_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, value_id: str,
                                       key: bytes, value: bytes, height: int, from_transaction: bool):
        try:
            limited_tracking = program_name == "credits.aleo" and mapping_name in ["committee", "bonded", "delegated"]
            if limited_tracking:
                conn = self.redis
                data = {
                    "key": key.hex(),
                    "value": value.hex(),
                }
                await conn.hset(f"{program_name}:{mapping_name}", key_id, json.dumps(data))

            if not limited_tracking or from_transaction:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']

                if not limited_tracking:
                    await cur.execute(
                        "INSERT INTO mapping_value (mapping_id, key_id, value_id, key, value) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (mapping_id, key_id) DO UPDATE SET value_id = %s, value = %s",
                        (mapping_id, key_id, value_id, key, value, value_id, value)
                    )

                await cur.execute(
                    "SELECT last_history_id FROM mapping_history_last_id WHERE key_id = %s",
                    (key_id,)
                )
                previous_id = res['last_history_id'] if (res := await cur.fetchone()) is not None else None

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction, previous_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "RETURNING id",
                    (mapping_id, height, key_id, key, value, from_transaction, previous_id)
                )
                if (res := await cur.fetchone()) is None:
                    raise ValueError("failed to insert mapping history")
                latest_id = res['id']
                await cur.execute(
                    "INSERT INTO mapping_history_last_id (key_id, last_history_id) VALUES (%s, %s) "
                    "ON CONFLICT (key_id) DO UPDATE SET last_history_id = %s",
                    (key_id, latest_id, latest_id)
                )

                if program_name == "credits.aleo" and mapping_name == "account":
                    address = str(Plaintext.load(BytesIO(key)))
                    plaintextvalue = cast(PlaintextValue, Value.load(BytesIO(value)))
                    plaintext = cast(LiteralPlaintext, plaintextvalue.plaintext)
                    public_balance = cast(int, plaintext.literal.primitive)
                    await cur.execute(
                        "INSERT INTO address (address, public_credits) VALUES (%s, %s) "
                        "ON CONFLICT (address) DO UPDATE SET public_credits = %s",
                        (address, public_balance, public_balance)
                    )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def remove_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, key: bytes, height: int,
                                       from_transaction: bool):
        try:
            limited_tracking = program_name == "credits.aleo" and mapping_name in ["committee", "bonded", "delegated"]
            if limited_tracking:
                conn = self.redis
                await conn.hdel(f"{program_name}:{mapping_name}", key_id)

            if not limited_tracking or from_transaction:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']
                if not limited_tracking:
                    await cur.execute(
                        "DELETE FROM mapping_value WHERE mapping_id = %s AND key_id = %s",
                        (mapping_id, key_id)
                    )

                await cur.execute(
                    "SELECT last_history_id FROM mapping_history_last_id WHERE key_id = %s",
                    (key_id,)
                )
                previous_id = res['last_history_id'] if (res := await cur.fetchone()) is not None else None

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction, previous_id) "
                    "VALUES (%s, %s, %s, %s, NULL, %s, %s) "
                    "RETURNING id",
                    (mapping_id, height, key_id, key, from_transaction, previous_id)
                )
                if (res := await cur.fetchone()) is None:
                    raise ValueError("failed to insert mapping history")
                latest_id = res['id']
                await cur.execute(
                    "INSERT INTO mapping_history_last_id (key_id, last_history_id) VALUES (%s, %s) "
                    "ON CONFLICT (key_id) DO UPDATE SET last_history_id = %s",
                    (key_id, latest_id, latest_id)
                )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def get_finalize_operations_by_height(self, height: int) -> list[FinalizeOperation]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT f.id, f.type FROM finalize_operation f "
                        "JOIN confirmed_transaction ct on ct.id = f.confirmed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE b.height = %s "
                        "ORDER BY f.id",
                        (height,)
                    )
                    data = await cur.fetchall()
                    result: list[FinalizeOperation] = []
                    for d in data:
                        if d["type"] == "UpdateKeyValue":
                            await cur.execute(
                                "SELECT mapping_id, key_id, value_id FROM finalize_operation_update_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            if u is None:
                                raise ValueError(f"finalize operation {d['id']} not found")
                            result.append(UpdateKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                                value_id=Field.loads(u["value_id"]),
                            ))
                        elif d["type"] == "RemoveKeyValue":
                            await cur.execute(
                                "SELECT mapping_id, key_id FROM finalize_operation_remove_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            if u is None:
                                raise ValueError(f"finalize operation {d['id']} not found")
                            result.append(RemoveKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                            ))
                    return result
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transaction_mapping_history_by_height(self, height: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT mh.id, m.program_id, m.mapping, m.mapping_id, mh.key_id, mh.key, mh.value FROM mapping_history mh "
                        "JOIN mapping m on mh.mapping_id = m.id "
                        "WHERE mh.height = %s AND mh.from_transaction = TRUE "
                        "ORDER BY mh.id",
                        (height,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_history_previous_value(self, history_id: int, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT previous_id FROM mapping_history WHERE id = %s",
                        (history_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    previous_id = res["previous_id"]
                    if previous_id is None:
                        return None
                    await cur.execute(
                        "SELECT key_id, value FROM mapping_history WHERE id = %s",
                        (previous_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    if res["key_id"] != key_id:
                        return None
                    return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise