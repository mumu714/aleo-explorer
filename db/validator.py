from __future__ import annotations

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .mapping import DatabaseMapping
from .address import DatabaseAddress

class DatabaseValidator(DatabaseBase):

    async def get_validator_count_at_height(self, height: int) -> Optional[int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s",
                        (height,)
                    )
                    res = await cur.fetchone()
                    if res:
                        return res["count"]
                    else:
                        return None
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validators_range_at_height(self, height: int, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT chm.address, chm.stake FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s "
                        "ORDER BY chm.stake DESC "
                        "LIMIT %s OFFSET %s",
                        (height, end - start, start)
                    )
                    validators = await cur.fetchall()
                    print(start, end)
                    await cur.execute("SELECT timestamp FROM block WHERE height = %s", (height,))
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return []
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT author, count(author) FROM va "
                        "GROUP BY author",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchall()
                    validator_counts = {v["author"]: v["count"] for v in res}
                    await cur.execute(
                        "SELECT count(*) FROM block WHERE timestamp > %s",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchone()
                    if res:
                        block_count = res["count"]
                    else:
                        return []
                    for validator in validators:
                        validator["uptime"] = validator_counts.get(validator["address"], 0) / block_count

                    return validators
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validator_uptime(self, address: str) -> Optional[float]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return None
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT author, count(author) FROM va "
                        "GROUP BY author",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchall()
                    validator_counts = {v["author"]: v["count"] for v in res}
                    if address not in validator_counts:
                        return 0
                    await cur.execute(
                        "SELECT count(*) FROM block WHERE timestamp > %s",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchone()
                    if res:
                        block_count = res["count"]
                    else:
                        return None
                    return validator_counts[address] / block_count
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_current_validator_count(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, COUNT(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "GROUP BY b.height ORDER BY b.height DESC LIMIT 1"
                    )
                    res = await cur.fetchone()
                    if res:
                        return res["count"]
                    else:
                        return 0
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_participation_rate(self) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return 0
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT count(author) FROM va",
                        (timestamp - 3600,)
                    )
                    res = await cur.fetchone()
                    if res:
                        validator_count = res["count"]
                    else:
                        return 0
                    await cur.execute(
                        "SELECT count(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "WHERE b.timestamp > %s",
                        (timestamp - 3600,)
                    )
                    res = await cur.fetchone()
                    if res:
                        total_validator_count = res["count"]
                    else:
                        return 0
                    return validator_count / total_validator_count
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    # returns: validators, all_validators_data
    async def get_validator_by_height(self, height: int) -> tuple[list[str], list[dict[str, Any]]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT author FROM dag_vertex dv "
                        "JOIN authority a on dv.authority_id = a.id "
                        "JOIN block b on a.block_id = b.id "
                        "WHERE b.height = %s ",
                        (height,)
                    )
                    validators = []
                    for row in await cur.fetchall():
                        validators.append(row["author"])
                    await cur.execute(
                        "SELECT chm.* FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s ORDER BY stake DESC",
                        (height,)
                    )
                    return validators, await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validators(self, start: int, end: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM committee_history ORDER BY height DESC LIMIT 1")
                    committee = await cur.fetchone()
                    if committee is None:
                        raise RuntimeError("no committee in database")
                    await cur.execute("SELECT * FROM committee_history_member WHERE committee_id = %s "
                                      "ORDER BY stake DESC LIMIT %s OFFSET %s",
                                      (committee["id"], end - start, start)
                            )
                    committee_member = await cur.fetchall()
                    return committee, committee_member
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validator_trend(self, address: str, timestamp: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT height, timestamp, committee_stake, stake_reward, delegate_reward FROM address_stake_reward "
                        "WHERE address = %s AND timestamp > %s "
                        "ORDER BY timestamp DESC ",
                        (address, timestamp)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validators_size(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM committee_history ORDER BY height DESC LIMIT 1")
                    committee = await cur.fetchone()
                    if committee is None:
                        raise RuntimeError("no committee in database")
                    await cur.execute("SELECT COUNT(*) FROM committee_history_member WHERE committee_id = %s",
                                      (committee["id"],))
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validator_bonds(self, address: str) -> list[dict[str, Any]]:
        try:
            stakers = await DatabaseMapping.get_bonded_mapping(self)
            all_delegators: list[dict[str, Any]] = []
            for delegator, (validator, stake) in stakers.items():
                if str(validator) == address:
                    stake_reward = await DatabaseAddress.get_address_stake_reward(self, address)
                    all_delegators.append({
                        "address": str(delegator),
                        "stake": int(stake),
                        "stake_reward": stake_reward
                    })
            return all_delegators
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def get_unbond_validator_by_address(self, address: str, height: int) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT * FROM mapping_bonded_history WHERE height = %s", (height-1,))
                    row = await cur.fetchone()
                    validator = None
                    if row is None:
                        return validator
                    address_bond: dict[str, Any] = {}
                    for content in row["content"].values():
                        key = Plaintext.load(BytesIO(bytes.fromhex(content["key"])))
                        if str(key) == address:
                            value = Value.load(BytesIO(bytes.fromhex(content["value"])))
                            if not isinstance(value, PlaintextValue):
                                raise RuntimeError("invalid bonded value")
                            plaintext = value.plaintext
                            if not isinstance(plaintext, StructPlaintext):
                                raise RuntimeError("invalid bonded value")
                            address_bond[str(plaintext["validator"])] = plaintext["microcredits"]
                    if len(address_bond) == 1:
                        validator = list(address_bond.keys())[0]
                    return validator
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_total_stake(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT total_stake FROM committee_history ORDER BY height DESC LIMIT 1")
                    committee = await cur.fetchone()
                    if committee is None:
                        raise RuntimeError("no committee in database")
                    return int(committee["total_stake"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise