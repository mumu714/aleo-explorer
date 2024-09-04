import asyncio
import os
import traceback
from sys import stdout
import time
import json
import requests

import rpc
from aleo_types import Block, BlockHash
from api import api
from db import Database
from interpreter.interpreter import init_builtin_program
# from node.light_node import LightNodeState
from node import Network
from node import Node
# from webapi import webapi
# from webui import webui
from .types import Request, Message, ExplorerRequest
from apscheduler.schedulers.tornado import TornadoScheduler # type: ignore
from aliyunsdkcore.client import AcsClient # type: ignore
from aliyunsdkcore.request import CommonRequest # type: ignore

class Explorer:

    def __init__(self):
        self.task = None
        self.message_queue: asyncio.Queue[Message] = asyncio.Queue()
        self.node = None
        self.db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                           database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                           redis_server=os.environ["REDIS_HOST"], redis_port=int(os.environ["REDIS_PORT"]),
                           redis_db=int(os.environ["REDIS_DB"]), redis_user=os.environ.get("REDIS_USER"),
                           redis_password=os.environ.get("REDIS_PASS"),
                           message_callback=self.message)

        # states
        self.dev_mode = False
        self.latest_height = 0
        self.latest_block_hash: BlockHash = Network.genesis_block.block_hash
        self.scheduler = TornadoScheduler()
        self.scheduler.start()

    def start(self):
        self.task = asyncio.create_task(self.main_loop())

    async def message(self, msg: Message):
        await self.message_queue.put(msg)

    async def node_request(self, request: ExplorerRequest):
        if isinstance(request, Request.GetLatestHeight):
            return self.latest_height
        elif isinstance(request, Request.ProcessUnconfirmedTransaction):
            await self.db.save_unconfirmed_transaction(request.tx)
        elif isinstance(request, Request.ProcessBlock):
            await self.add_block(request.block)
        elif isinstance(request, Request.GetBlockByHeight):
            return await self.db.get_block_by_height(request.height)
        elif isinstance(request, Request.GetBlockHashByHeight):
            if request.height == self.latest_height:
                return self.latest_block_hash
            return await self.db.get_block_hash_by_height(request.height)
        elif isinstance(request, Request.GetBlockHeaderByHeight):
            return await self.db.get_block_header_by_height(request.height)
        elif isinstance(request, Request.RevertToBlock):
            raise NotImplementedError
        elif isinstance(request, Request.GetDevMode):
            return self.dev_mode
        else:
            print("unhandled explorer request")

    async def check_genesis(self):
        height = await self.db.get_latest_height()
        if height is None:
            if self.dev_mode:
                await self.add_block(Network.dev_genesis_block)
            else:
                await self.add_block(Network.genesis_block)

    async def main_loop(self):
        try:
            await self.db.connect()
            await self.db.migrate()
            await self.check_clear()
            await self.check_dev_mode()
            await self.check_genesis()
            await self.check_revert()
            latest_height = await self.db.get_latest_height()
            if latest_height is None:
                raise ValueError("no block in database")
            self.latest_height = latest_height
            latest_block_hash = await self.db.get_block_hash_by_height(self.latest_height)
            if latest_block_hash is None:
                raise ValueError("no block in database")
            self.latest_block_hash = latest_block_hash
            print(f"latest height: {self.latest_height}")
            self.node = Node(explorer_message=self.message, explorer_request=self.node_request)
            await self.node.connect(os.environ.get("P2P_NODE_HOST", "127.0.0.1"), int(os.environ.get("P2P_NODE_PORT", "4133")))
            # _ = asyncio.create_task(webapi.run())
            # _ = asyncio.create_task(webui.run())
            # _ = asyncio.create_task(api.run())
            asyncio.create_task(rpc.run())
            self.scheduler.add_job(self.add_hashrate, 'cron', minute="*/5", id='job1')  # type: ignore
            self.scheduler.add_job(self.add_coinbase, 'cron', hour="*/12", id='job3')  # type: ignore
            self.scheduler.add_job(self.update_24H_reward_data, 'cron', hour="*/1", id='job4')  # type: ignore
            self.scheduler.add_job(self.check_data_sync, 'cron', minute="*/10", id='job5')  # type: ignore
            while True:
                msg = await self.message_queue.get()
                match msg.type:
                    case Message.Type.NodeConnectError:
                        print("node connect error:", msg.data)
                    case Message.Type.NodeConnected:
                        print("node connected")
                    case Message.Type.NodeDisconnected:
                        print("node disconnected")
                    case Message.Type.DatabaseConnectError:
                        print("database connect error:", msg.data)
                    case Message.Type.DatabaseConnected:
                        print("database connected")
                    case Message.Type.DatabaseDisconnected:
                        print("database disconnected")
                    case Message.Type.DatabaseError:
                        print("database error:", msg.data)
                    case Message.Type.DatabaseBlockAdded:
                        # maybe do something later?
                        pass
        except Exception as e:
            print("explorer error:", e)
            traceback.print_exc()
            raise

    async def add_block(self, block: Block):
        if block in [Network.genesis_block, Network.dev_genesis_block]:
            for program in Network.builtin_programs:
                await init_builtin_program(self.db, program)
            await self.db.save_block(block)
            return
        if block.previous_hash != self.latest_block_hash:
            print(f"ignoring block {block} because previous block hash does not match")
        else:
            print(f"adding block {block}")
            await self.db.save_block(block)
            self.latest_height = block.header.metadata.height
            self.latest_block_hash = block.block_hash

    async def get_latest_block(self):
        return await self.db.get_latest_block()

    async def check_dev_mode(self):
        dev_mode = os.environ.get("DEV_MODE", "")
        if dev_mode == "1":
            self.dev_mode = True

        if await self.db.get_latest_height() is not None:
            db_genesis = await self.db.get_block_by_height(0)
            if db_genesis is None:
                return
            if self.dev_mode:
                genesis_block = Network.dev_genesis_block
            else:
                genesis_block = Network.genesis_block
            if db_genesis.header.transactions_root != genesis_block.header.transactions_root:
                await self.clear_database()

    async def add_hashrate(self):
        await self.db.save_hashrate()

    async def add_coinbase(self):
        await self.db.save_one_day_coinbase()

    async def update_24H_reward_data(self):
        await self.db.save_24H_reward_data()

    async def check_data_sync(self):
        last_timestamp, last_height = await asyncio.gather(
            self.db.get_latest_block_timestamp(),
            self.db.get_latest_height()
        )
        now = int(time.time())
        out_of_sync = now - last_timestamp > 300
        if out_of_sync:
            rpc_root = os.environ.get("RPC_URL_ROOT", "127.0.0.1:3003")
            node_height = requests.get(f"{rpc_root}/testnet/latest/height")
            print(f"Curret Aleo.Info block height at {last_height}, Node height at {node_height.text}")
            message = f"Aleo.Info 后端报错信息: 当前浏览器区块落后超过5分钟,请检查. Current Aleo.Info block height at {last_height}, Node height at {node_height.text}"
            self.send_lark_message(message)
            self.single_call()

    async def clear_database(self):
        print("The current database has a different genesis block!\nPress Ctrl+C to abort, or wait 10 seconds to clear the database.")
        i = 10
        while i > 0:
            print(f"\x1b[G\x1b[2K!!! Clearing database in {i} seconds !!!", end="")
            stdout.flush()
            await asyncio.sleep(1)
            i -= 1
        print("\x1b[G\x1b[2K!!! Clearing database now !!!")
        stdout.flush()
        await self.db.clear_database()

    async def check_revert(self):
        if os.path.exists("revert_flag") and os.path.isfile("revert_flag"):
            try:
                os.remove("revert_flag")
            except OSError as e:
                print("Cannot remove revert_flag:", e)
            await self.db.revert_to_last_backup()

    async def check_clear(self):
        if os.path.exists("clear_flag") and os.path.isfile("clear_flag"):
            try:
                os.remove("clear_flag")
            except OSError as e:
                print("Cannot remove clear_flag:", e)
            await self.db.clear_database()

    def send_lark_message(self, messgae: str):
        webhook_url = os.environ.get("LARK_URL")
        if webhook_url is None:
            raise ValueError("invalid lark webhook_url")
        data = {
            "msg_type": "text",
            "content": {
                "text": messgae
            }
        }
        response = requests.post(
            url=webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(data)
        )
        if response.status_code == 200:
            print("[Aleo.Info Check Sync] Message sent successfully!")
        else:
            print(f"[Aleo.Info Check Sync] Failed to send message: {response.text}")
        
    def single_call(self):
        access_key_id = os.environ.get("ACCESSID")
        access_key_secret = os.environ.get("ACCESSSECRET")
        region_id = os.environ.get("REGION_ID")
        called_number = os.environ.get("CALLED_NUMBER")
        TtsCode = os.environ.get("TTSTemplate")
        if (access_key_id is None 
            or access_key_secret is None 
            or region_id is None 
            or called_number is None 
            or TtsCode is None
        ):
            raise ValueError("invalid value")
        client = AcsClient(access_key_id, access_key_secret, region_id)
        request = CommonRequest(domain='dyvmsapi.aliyuncs.com', version='2017-05-25',action_name='SingleCallByTts')
        request.set_accept_format('json') # type: ignore
        request.set_method('POST') # type: ignore
        request.add_query_param('RegionId', region_id) # type: ignore
        request.add_query_param('CalledShowNumber', '')   # type: ignore
        request.add_query_param('CalledNumber', called_number)   # type: ignore
        request.add_query_param('TtsCode', TtsCode)   # type: ignore
        data = {
            "minerId": 'Aleo Info',
            "errorDescribe": 'Explorer Error',
        }
        request.add_query_param('TtsParam', json.dumps(data))  # type: ignore

        response = client.do_action_with_exception(request) # type: ignore
        print("[Aleo.Info Check Sync] Call Response", str(response, encoding='utf-8')) # type: ignore
    