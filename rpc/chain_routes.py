from io import BytesIO
from typing import Any, cast, Optional, ParamSpec, TypeVar, Callable, Awaitable

import aleo_explorer_rust
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.responses import JSONResponse

import util.arc0137
from aleo_types import u32, Transition, ExecuteTransaction, PrivateTransitionInput, \
    RecordTransitionInput, TransitionOutput, RecordTransitionOutput, DeployTransaction, PublicTransitionInput, \
    PublicTransitionOutput, PrivateTransitionOutput, ExternalRecordTransitionInput, \
    ExternalRecordTransitionOutput, AcceptedDeploy, AcceptedExecute, RejectedExecute, \
    FeeTransaction, RejectedDeploy, RejectedExecution, Identifier, Entry, FutureTransitionOutput, Future, \
    PlaintextArgument, FutureArgument, StructPlaintext, Finalize, \
    PlaintextFinalizeType, StructPlaintextType, UpdateKeyValue, Value, Plaintext, RemoveKeyValue, FinalizeOperation, \
    FeeComponent, Fee, Option
from aleo_types.cached import cached_get_key_id, cached_get_mapping_id
from db import Database
from util.global_cache import get_program
from util.typing_exc import Unreachable
from .classes import UIAddress
from .utils import function_signature, get_future_argument
from .format import *

try:
    from line_profiler import profile
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await func(*args, **kwargs)
        return wrapper

DictList = list[dict[str, Any]]

@profile
async def block_route(request: Request):
    db: Database = request.app.state.db
    height = request.query_params.get("h")
    block_hash = request.query_params.get("bh")
    if height is None and block_hash is None:
        raise HTTPException(status_code=400, detail="Missing height or block hash")
    if height is not None:
        block = await db.get_block_by_height(u32(int(height)))
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        block_hash = block.block_hash
    elif block_hash is not None:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        height = block.header.metadata.height
    else:
        raise RuntimeError("unreachable")
    height = int(height)

    coinbase_reward = await db.get_block_coinbase_reward_by_height(height)
    txs: DictList = []
    total_base_fee = 0
    total_priority_fee = 0
    total_burnt_fee = 0
    for ct in block.transactions.transactions:
        # TODO: use proper fee calculation
        # fee_breakdown = await ct.get_fee_breakdown(db)
        fee = ct.transaction.fee
        if isinstance(fee, Fee):
            base_fee, priority_fee = fee.amount
        elif fee.value is not None:
            base_fee, priority_fee = fee.value.amount
        else:
            base_fee, priority_fee = 0, 0
        fee_breakdown = FeeComponent(base_fee, 0, [0], priority_fee, 0)
        print(fee_breakdown)
        base_fee = fee_breakdown.storage_cost + fee_breakdown.namespace_cost + sum(fee_breakdown.finalize_costs)
        priority_fee = fee_breakdown.priority_fee
        burnt_fee = fee_breakdown.burnt
        total_base_fee += base_fee
        total_priority_fee += priority_fee
        total_burnt_fee += burnt_fee
        if isinstance(ct, AcceptedDeploy):
            tx = ct.transaction
            if not isinstance(tx, DeployTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            t = {
                "tx_id": str(tx.id),
                "index": ct.index,
                "type": "Deploy",
                "state": "Accepted",
                "transitions_count": 1,
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "program_id": tx.deployment.program.id,
            }
            txs.append(t)
        elif isinstance(ct, AcceptedExecute):
            tx = ct.transaction
            if not isinstance(tx, ExecuteTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            fee = cast(Option[Fee], tx.fee).value
            if fee is not None:
                base_fee, priority_fee = fee.amount
            else:
                base_fee, priority_fee = 0, 0
            root_transition = tx.execution.transitions[-1]
            t = {
                "tx_id": str(tx.id),
                "index": ct.index,
                "type": "Execute",
                "state": "Accepted",
                "transitions_count": len(tx.execution.transitions) + bool(fee is not None),
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "root_transition": f"{root_transition.program_id}/{root_transition.function_name}",
            }
            txs.append(t)
        elif isinstance(ct, RejectedExecute):
            tx = ct.transaction
            if not isinstance(tx, FeeTransaction):
                raise HTTPException(status_code=550, detail="Invalid transaction type")
            base_fee, priority_fee = cast(Fee, tx.fee).amount
            rejected = ct.rejected
            if not isinstance(rejected, RejectedExecution):
                raise HTTPException(status_code=550, detail="Invalid rejected transaction type")
            root_transition = rejected.execution.transitions[-1]
            t = {
                "tx_id": str(tx.id),
                "index": ct.index,
                "type": "Execute",
                "state": "Rejected",
                "transitions_count": 1,
                "base_fee": base_fee - burnt_fee,
                "priority_fee": priority_fee,
                "burnt_fee": burnt_fee,
                "root_transition": f"{root_transition.program_id}/{root_transition.function_name}",
            }
            txs.append(t)
        else:
            raise HTTPException(status_code=550, detail="Unsupported transaction type")

    validators, all_validators_raw = await db.get_validator_by_height(height)
    all_validators: list[UIAddress] = []
    for v in all_validators_raw:
        all_validators.append(await UIAddress(v["address"]).resolve(db))

    subs: DictList = []
    if isinstance(block.authority, QuorumAuthority):
        subdag = block.authority.subdag
        for round_, certificates in subdag.subdag.items():
            for index, certificate in enumerate(certificates):
                if round_ != certificate.batch_header.round:
                    raise ValueError("invalid subdag round")
                else:
                    signatures = [str(i) for i in certificate.signatures]
                    subs.append({
                        "round": round_,
                        "index": index,
                        "committee_id": str(certificate.batch_header.committee_id),
                        "batch_id": str(certificate.batch_header.batch_id),
                        "author": str(certificate.batch_header.author),
                        "timestamp": certificate.batch_header.timestamp,
                        "previous_certificate_ids": [str(i) for i in certificate.batch_header.previous_certificate_ids],
                        "transmission_ids": [str(i.id) for i in certificate.batch_header.transmission_ids], # type: ignore
                        "batch_header_signature": str(certificate.batch_header.signature),
                        "signatures": signatures 
                    })

    ctx = {
        "block": format_block(block),
        "block_hash_trunc": str(block_hash)[:12] + "..." + str(block_hash)[-6:],
        "coinbase_reward": str(coinbase_reward),
        "transactions": txs,
        "total_base_fee": total_base_fee,
        "total_priority_fee": total_priority_fee,
        "total_burnt_fee": total_burnt_fee,
        "subdag": subs,
        "validators": validators,
        "all_validators": [v.address for v in all_validators],
    }
    return JSONResponse(ctx)

async def block_solution_route(request: Request):
    db: Database = request.app.state.db
    height = request.query_params.get("h")
    block_hash = request.query_params.get("bh")
    if height is None and block_hash is None:
        raise HTTPException(status_code=400, detail="Missing height or block hash")
    if height is not None:
        block = await db.get_block_by_height(u32(int(height)))
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        block_hash = block.block_hash
    elif block_hash is not None:
        block = await db.get_block_by_hash(block_hash)
        if block is None:
            raise HTTPException(status_code=404, detail="Block not found")
        height = block.header.metadata.height
    else:
        raise RuntimeError("unreachable")
    height = int(height)
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    solution_count = await db.get_solution_count_by_height(height)
    total_pages = (solution_count // 10) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 10 * (page - 1)
    css: DictList = []
    target_sum = await db.get_solution_total_target_by_height(height)
    solutions = await db.get_solution_by_height(height, start, start + 10)
    for solution in solutions:
        css.append({
            "address": solution["address"],
            "address_trunc": solution["address"][:15] + "..." + solution["address"][-10:],
            "counter": str(solution["counter"]),
            "solution_id": solution["solution_id"],
            "target": str(solution["target"]),
            "reward": solution["reward"],
        })
    ctx = {
        "height": height,
        "block_hash_trunc": str(block_hash)[:12] + "..." + str(block_hash)[-6:],
        "solutions": css,
        "target_sum": int(target_sum),
        "page": page,
        "solution_count": solution_count,
    }
    return JSONResponse(ctx)

async def transaction_route(request: Request):
    db: Database = request.app.state.db
    tx_id = request.query_params.get("id")
    if tx_id is None:
        raise HTTPException(status_code=400, detail="Missing transaction id")
    tx_id = await db.get_updated_transaction_id(tx_id)
    is_confirmed = await db.is_transaction_confirmed(tx_id)
    if is_confirmed is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if is_confirmed:
        confirmed_transaction = await db.get_confirmed_transaction(tx_id)
        if confirmed_transaction is None:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        transaction = confirmed_transaction.transaction
    else:
        confirmed_transaction = None
        transaction = await db.get_unconfirmed_transaction(tx_id)
        if transaction is None:
            raise HTTPException(status_code=404, detail="Transaction not found")

    first_seen = await db.get_transaction_first_seen(tx_id)
    index = -1
    original_txid: Optional[str] = None
    program_info: Optional[dict[str, Any]] = None
    if isinstance(transaction, DeployTransaction):
        transaction_type = "Deploy"
        if is_confirmed:
            transaction_state = "Accepted"
            if confirmed_transaction is None:
                raise Unreachable
            index = confirmed_transaction.index
        else:
            transaction_state = "Unconfirmed"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
            if program_info is None:
                raise HTTPException(status_code=550, detail="Database inconsistent")
    elif isinstance(transaction, ExecuteTransaction):
        transaction_type = "Execute"
        if is_confirmed:
            transaction_state = "Accepted"
            if confirmed_transaction is None:
                raise Unreachable
            index = confirmed_transaction.index
        else:
            transaction_state = "Unconfirmed"
    elif isinstance(transaction, FeeTransaction):
        if confirmed_transaction is None:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        index = confirmed_transaction.index
        if isinstance(confirmed_transaction, RejectedDeploy):
            transaction_type = "Deploy"
            transaction_state = "Rejected"
            program_info = await db.get_deploy_transaction_program_info(tx_id)
        elif isinstance(confirmed_transaction, RejectedExecute):
            transaction_type = "Execute"
            transaction_state = "Rejected"
        else:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        original_txid = await db.get_rejected_transaction_original_id(tx_id)
    else:
        raise HTTPException(status_code=550, detail="Unsupported transaction type")

    # TODO: use proper fee calculation
    if confirmed_transaction is None:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await transaction.get_fee_breakdown(db)
        block = None
        block_confirm_time = None
    else:
        # storage_cost, namespace_cost, finalize_costs, priority_fee, burnt = await confirmed_transaction.get_fee_breakdown(db)
        block = await db.get_block_from_transaction_id(tx_id)
        if block is None:
            raise HTTPException(status_code=550, detail="Database inconsistent")
        block_confirm_time = await db.get_block_confirm_time(block.height)

    fee = transaction.fee
    if isinstance(fee, Fee):
        storage_cost, priority_fee = fee.amount
    elif fee.value is not None:
        storage_cost, priority_fee = fee.value.amount
    else:
        storage_cost, priority_fee = 0, 0
    namespace_cost = 0
    finalize_costs: list[int] = []
    burnt = 0

    ctx: dict[str, Any] = {
        "tx_id": tx_id,
        "tx_id_trunc": str(tx_id)[:12] + "..." + str(tx_id)[-6:],
        "block": format_block(block) if block else block,
        "block_confirm_time": block_confirm_time,
        "index": index,
        "type": transaction_type,
        "state": transaction_state,
        "transaction_type": "",
        "total_fee": storage_cost + namespace_cost + sum(finalize_costs) + priority_fee + burnt,
        "storage_cost": storage_cost,
        "namespace_cost": namespace_cost,
        "finalize_costs": finalize_costs,
        "priority_fee": priority_fee,
        "burnt_fee": burnt,
        "first_seen": first_seen,
        "original_txid": original_txid,
        "program_info": program_info,
        "reject_reason": await db.get_transaction_reject_reason(tx_id) if transaction_state == "Rejected" else None,
    }

    if isinstance(transaction, DeployTransaction):
        deployment = transaction.deployment
        program = deployment.program
        fee_transition = cast(Fee, transaction.fee).transition
        ctx.update({
            "edition": int(deployment.edition),
            "program_id": str(program.id),
            "transitions": [{
                "transition_id": str(fee_transition.id),
                "action": f"{fee_transition.program_id}/{fee_transition.function_name}",
            }],
        })
    elif isinstance(transaction, ExecuteTransaction):
        global_state_root = transaction.execution.global_state_root
        proof = transaction.execution.proof.value
        transitions: DictList = []

        for transition in transaction.execution.transitions:
            transitions.append({
                "transition_id": str(transition.id),
                "action":f"{transition.program_id}/{transition.function_name}",
            })
            if transition.program_id == "credits.aleo" and transition.function_name == "transfer_public":
                output = cast(FutureTransitionOutput, transition.outputs[0])
                future = cast(Future, output.future.value)
                transfer_from = str(Database.get_primitive_from_argument_unchecked(future.arguments[0]))
                transfer_to = str(Database.get_primitive_from_argument_unchecked(future.arguments[1]))
                amount = int(cast(int, Database.get_primitive_from_argument_unchecked(future.arguments[2])))
                ctx["transfer_detail"] = {
                    "transfer_from": transfer_from,
                    "transfer_to": transfer_to,
                    "amount": amount
                }
                ctx.update({"transaction_type": "transfer"})
        fee = cast(Option[Fee], transaction.fee).value
        if fee is not None:
            transition = fee.transition
            fee_transition = {
                "transition_id": str(transition.id),
                "action":f"{transition.program_id}/{transition.function_name}",
            }
        else:
            fee_transition = None
        ctx.update({
            "global_state_root": str(global_state_root),
            "proof": str(proof),
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "transitions": transitions,
            "fee_transition": fee_transition,
        })
    elif isinstance(transaction, FeeTransaction): # type: ignore[reportUnnecessaryIsInstance] # future proof
        fee = cast(Fee, transaction.fee)
        global_state_root = fee.global_state_root
        proof = fee.proof.value
        transitions = []
        rejected_transitions: DictList = []
        transition = fee.transition
        transitions.append({
            "transition_id": str(transition.id),
            "action":f"{transition.program_id}/{transition.function_name}",
        })
        if isinstance(confirmed_transaction, RejectedExecute):
            rejected = confirmed_transaction.rejected
            if not isinstance(rejected, RejectedExecution):
                raise HTTPException(status_code=550, detail="invalid rejected transaction")
            for transition in rejected.execution.transitions:
                transition: Transition
                rejected_transitions.append({
                    "transition_id": str(transition.id),
                    "action": f"{transition.program_id}/{transition.function_name}",
                })
        else:
            raise HTTPException(status_code=550, detail="Unsupported transaction type")
        ctx.update({
            "global_state_root": str(global_state_root),
            "proof": str(proof),
            "proof_trunc": str(proof)[:30] + "..." + str(proof)[-30:] if proof else None,
            "transitions": transitions,
            "rejected_transitions": rejected_transitions,
        })

    else:
        raise HTTPException(status_code=550, detail="Unsupported transaction type")

    mapping_operations: Optional[list[dict[str, Any]]] = None
    if confirmed_transaction is not None:
        if block is None:
            raise Unreachable
        limited_tracking = {
            cached_get_mapping_id("credits.aleo", "committee"): ("credits.aleo", "committee"),
            cached_get_mapping_id("credits.aleo", "bonded"): ("credits.aleo", "bonded"),
        }
        fos: list[FinalizeOperation] = []
        untracked_fos: list[FinalizeOperation] = []
        for ct in block.transactions:
            for fo in ct.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    if str(fo.mapping_id) in limited_tracking:
                        untracked_fos.append(fo)
                    else:
                        fos.append(fo)
        mhs = await db.get_transaction_mapping_history_by_height(block.height)
        # TODO: remove compatibility after mainnet
        after_tracking = False
        if len(fos) + len(untracked_fos) == len(mhs):
            after_tracking = True
            fos = []
            for ct in block.transactions:
                for fo in ct.finalize:
                    if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                        fos.append(fo)
        if len(fos) == len(mhs):
            indices: list[int] = []
            untracked_indices: list[int] = []
            last_index = -1
            for fo in confirmed_transaction.finalize:
                if isinstance(fo, (UpdateKeyValue, RemoveKeyValue)):
                    if not after_tracking and fo in untracked_fos:
                        untracked_indices.append(untracked_fos.index(fo))
                    else:
                        last_index = fos.index(fo, last_index + 1)
                        indices.append(last_index)
            mapping_operations: Optional[list[dict[str, Any]]] = []
            for i in untracked_indices:
                fo = untracked_fos[i]
                program_id, mapping_name = limited_tracking[str(fo.mapping_id)]
                if isinstance(fo, UpdateKeyValue):
                    mapping_operations.append({
                        "type": "Update",
                        "program_id": program_id,
                        "mapping_name": mapping_name,
                        "key": None,
                        "value": None,
                        "previous_value": None,
                    })
                elif isinstance(fo, RemoveKeyValue):
                    mapping_operations.append({
                        "type": "Remove",
                        "program_id": program_id,
                        "mapping_name": mapping_name,
                        "key": None,
                        "value": None,
                        "previous_value": None,
                    })
            for i in indices:
                fo = fos[i]
                mh = mhs[i]
                if str(fo.mapping_id) != str(mh["mapping_id"]):
                    mapping_operations = None
                    break
                limited_tracked = str(fo.mapping_id) in limited_tracking
                if isinstance(fo, UpdateKeyValue):
                    if mh["value"] is None:
                        mapping_operations = None
                        break
                    key_id = cached_get_key_id(mh["program_id"], mh["mapping"], mh["key"])
                    value_id = aleo_explorer_rust.get_value_id(str(key_id), mh["value"])
                    if value_id != str(fo.value_id):
                        mapping_operations = None
                        break
                    if limited_tracked:
                        previous_value = None
                    else:
                        previous_value = await db.get_mapping_history_previous_value(mh["id"], mh["key_id"])
                    if previous_value is not None:
                        previous_value = str(Value.load(BytesIO(previous_value)))
                    mapping_operations.append({
                        "type": "Update",
                        "program_id": mh["program_id"],
                        "mapping_name": mh["mapping"],
                        "key": str(Plaintext.load(BytesIO(mh["key"]))),
                        "value": str(Value.load(BytesIO(mh["value"]))),
                        "previous_value": previous_value,
                        "limited_tracked": limited_tracked,
                    })
                elif isinstance(fo, RemoveKeyValue):
                    if mh["value"] is not None:
                        mapping_operations = None
                        break
                    if limited_tracked:
                        previous_value = None
                    else:
                        previous_value = await db.get_mapping_history_previous_value(mh["id"], mh["key_id"])
                    if previous_value is not None:
                        previous_value = str(Value.load(BytesIO(previous_value)))
                    elif not limited_tracked:
                        mapping_operations = None
                        break
                    mapping_operations.append({
                        "type": "Remove",
                        "program_id": mh["program_id"],
                        "mapping_name": mh["mapping"],
                        "key": str(Plaintext.load(BytesIO(mh["key"]))),
                        "previous_value": previous_value,
                        "limited_tracked": limited_tracked,
                    })

    ctx["mapping_operations"] = mapping_operations

    return JSONResponse(ctx)

async def transactions_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 10
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    transaction_count = await db.get_transaction_count()
    if offset > transaction_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    transactions_data = await db.get_transactions(start, start + limit)
    ctx = {
        "transactions": transactions_data,
        "totalCount": transaction_count
    }
    return JSONResponse(ctx)

async def transition_route(request: Request):
    db: Database = request.app.state.db
    ts_id = request.query_params.get("id")
    if ts_id is None:
        raise HTTPException(status_code=400, detail="Missing transition id")
    block = await db.get_block_from_transition_id(ts_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Transition not found")

    transaction_id = None
    transition = None
    state = ""
    for ct in block.transactions:
        if transition is not None:
            break
        match ct:
            case AcceptedDeploy():
                tx = ct.transaction
                if not isinstance(tx, DeployTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                state = "Accepted"
                fee = cast(Fee, tx.fee)
                if str(fee.transition.id) == ts_id:
                    transition = fee.transition
                    transaction_id = tx.id
                    break
            case AcceptedExecute():
                tx = ct.transaction
                if not isinstance(tx, ExecuteTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                state = "Accepted"
                for ts in tx.execution.transitions:
                    if str(ts.id) == ts_id:
                        transition = ts
                        transaction_id = tx.id
                        break
                fee = cast(Option[Fee], tx.fee).value
                if transaction_id is None and fee is not None:
                    ts = fee.transition
                    if str(ts.id) == ts_id:
                        transition = ts
                        transaction_id = tx.id
                        break
            case RejectedExecute():
                tx = ct.transaction
                if not isinstance(tx, FeeTransaction):
                    raise HTTPException(status_code=550, detail="Database inconsistent")
                fee = cast(Fee, tx.fee)
                if str(fee.transition.id) == ts_id:
                    transition = fee.transition
                    transaction_id = tx.id
                    state = "Accepted"
                else:
                    rejected = ct.rejected
                    if not isinstance(rejected, RejectedExecution):
                        raise HTTPException(status_code=550, detail="Database inconsistent")
                    for ts in rejected.execution.transitions:
                        if str(ts.id) == ts_id:
                            transition = ts
                            transaction_id = tx.id
                            state = "Rejected"
                            break
            case _:
                raise HTTPException(status_code=550, detail="Not implemented")
    if transaction_id is None:
        raise HTTPException(status_code=550, detail="Transition not found in block")
    transition = cast(Transition, transition)

    program_id = transition.program_id
    function_name = transition.function_name
    tpk = transition.tpk
    tcm = transition.tcm

    inputs: DictList = []
    for input_ in transition.inputs:
        if isinstance(input_, PublicTransitionInput):
            inputs.append({
                "type": "Public",
                "id": str(input_.plaintext_hash),
                "value": str(input_.plaintext.value),
            })
        elif isinstance(input_, PrivateTransitionInput):
            inputs.append({
                "type": "Private",
                "id": str(input_.ciphertext_hash),
                "value": str(input_.ciphertext.value),
            })
        elif isinstance(input_, RecordTransitionInput):
            inputs.append({
                "type": "Record",
                "id": str(input_.serial_number),
                "tag": str(input_.tag),
            })
        elif isinstance(input_, ExternalRecordTransitionInput):
            inputs.append({
                "type": "External record",
                "id": str(input_.input_commitment),
            })
        else:
            raise HTTPException(status_code=550, detail="Not implemented")

    outputs: DictList = []
    self_future: Optional[Future] = None
    for output in transition.outputs:
        output: TransitionOutput
        if isinstance(output, PublicTransitionOutput):
            outputs.append({
                "type": "Public",
                "id": str(output.plaintext_hash),
                "value": str(output.plaintext.value),
            })
        elif isinstance(output, PrivateTransitionOutput):
            outputs.append({
                "type": "Private",
                "id": str(output.ciphertext_hash),
                "value": str(output.ciphertext.value),
            })
        elif isinstance(output, RecordTransitionOutput):
            output_data: dict[str, Any] = {
                "type": "Record",
                "id": str(output.commitment),
                "checksum": str(output.checksum),
                "value": str(output.record_ciphertext.value),
            }
            record = output.record_ciphertext.value
            if record is not None:
                record_data: dict[str, Any] = {
                    "owner": str(record.owner),
                }
                data: list[tuple[Identifier, Entry[Any]]] = []
                for identifier, entry in record.data:
                    data.append((str(identifier), str(entry))) # type: ignore
                record_data["data"] = data
                output_data["record_data"] = record_data
            outputs.append(output_data)
        elif isinstance(output, ExternalRecordTransitionOutput):
            outputs.append({
                "type": "External record",
                "id": str(output.commitment),
            })
        elif isinstance(output, FutureTransitionOutput):
            future = output.future.value
            if future is not None:
                if future.program_id == program_id and future.function_name == function_name:
                    self_future = future
                arguments:list[Any]= []
                for arg in future.arguments:
                    arguments.append(get_future_argument(arg))
                future = {
                    "program_id": str(future.program_id),
                    "function_name": str(future.function_name),
                    "arguments": arguments
                }
            outputs.append({
                "type": "Future",
                "id": str(output.future_hash),
                "future": future,
            })
        else:
            raise HTTPException(status_code=550, detail="Not implemented")

    finalizes: list[dict[str, str]] = []
    if self_future is not None:
        for i, argument in enumerate(self_future.arguments):
            if isinstance(argument, PlaintextArgument):
                struct_type = ""
                if isinstance(argument.plaintext, StructPlaintext):
                    program = await get_program(db, str(transition.program_id))
                    if program is None:
                        raise HTTPException(status_code=550, detail="Program not found")
                    finalize = cast(Finalize, program.functions[transition.function_name].finalize.value)
                    finalize_type = cast(PlaintextFinalizeType, finalize.inputs[i].finalize_type)
                    struct_type = str(cast(StructPlaintextType, finalize_type.plaintext_type).struct)
                finalizes.append({
                    "type": "Plaintext",
                    "struct_type": struct_type,
                    "value": str(argument.plaintext)
                })
            elif isinstance(argument, FutureArgument):
                future = argument.future
                finalizes.append({
                    "type": "Future",
                    "value": f"{future.program_id}/{future.function_name}(...)",
                })

    ctx = {
        "ts_id": ts_id,
        "ts_id_trunc": str(ts_id)[:12] + "..." + str(ts_id)[-6:],
        "transaction_id": str(transaction_id),
        "state": state,
        "program_id": str(program_id),
        "function_name": str(function_name),
        "tpk": str(tpk),
        "tcm": str(tcm),
        "function_signature": await function_signature(db, str(transition.program_id), str(transition.function_name)),
        "inputs": inputs,
        "outputs": outputs,
        "finalizes": finalizes,
    }
    return JSONResponse(ctx)

async def transitions_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 10
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    transition_count = await db.get_transition_count()
    if offset > transition_count:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    transitions_data = await db.get_transitions(start, start + limit)
    ctx = {
        "transitions": transitions_data,
        "totalCount": transition_count
    }
    return JSONResponse(ctx)

async def solution_route(request: Request):
    db: Database = request.app.state.db
    solution_id = request.query_params.get("id")
    if not solution_id:
        return HTTPException(400, "Missing Solution Id")
    solution = await db.get_puzzle_commitment(solution_id)
    ctx = {
        "solution": format_number(solution) if solution else None
    }
    return JSONResponse(ctx)

async def search_route(request: Request):
    db: Database = request.app.state.db
    query = request.query_params.get("q")
    if query is None:
        raise HTTPException(status_code=400, detail="Missing query")
    query = query.lower().strip()
    remaining_query = dict(request.query_params)
    del remaining_query["q"]
    if remaining_query:
        remaining_query = "&" + "&".join([f"{k}={v}" for k, v in remaining_query.items()])
    else:
        remaining_query = ""
    try:
        height = int(query)
        return RedirectResponse(f"/block?h={height}{remaining_query}", status_code=302)
    except ValueError:
        pass
    if query.startswith("aprivatekey1zkp"):
        raise HTTPException(status_code=400, detail=">>> YOU HAVE LEAKED YOUR PRIVATE KEY <<< Please throw it away and generate a new one.")
    elif query.startswith("ab1"):
        # block hash
        blocks = await db.search_block_hash(query)
        if not blocks:
            raise HTTPException(status_code=404, detail="Block not found")
        if len(blocks) == 1:
            return RedirectResponse(f"/block?bh={blocks[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(blocks) > 50:
            blocks = blocks[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "block",
            "blocks": blocks,
            "too_many": too_many,
        }
        return JSONResponse(ctx)
    elif query.startswith("at1"):
        # transaction id
        transactions = await db.search_transaction_id(query)
        if not transactions:
            raise HTTPException(status_code=404, detail="Transaction not found")
        if len(transactions) == 1:
            return RedirectResponse(f"/transaction?id={transactions[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(transactions) > 50:
            transactions = transactions[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "transaction",
            "transactions": transactions,
            "too_many": too_many,
        }
        return JSONResponse(ctx)
    elif query.startswith("au1"):
        # transition id
        transitions = await db.search_transition_id(query)
        if not transitions:
            raise HTTPException(status_code=404, detail="Transition not found")
        if len(transitions) == 1:
            return RedirectResponse(f"/transition?id={transitions[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(transitions) > 50:
            transitions = transitions[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "transition",
            "transitions": transitions,
            "too_many": too_many,
        }
        return JSONResponse(ctx)
    elif query.startswith("aleo1"):
        # address
        addresses = await db.search_address(query)
        if not addresses:
            if len(query) == 63 and query.isalnum():
                return RedirectResponse(f"/address?a={query}{remaining_query}", status_code=302)
            else:
                raise HTTPException(status_code=404, detail=f"Address format error.")
        if len(addresses) == 1:
            return RedirectResponse(f"/address?a={addresses[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(addresses) > 50:
            addresses = addresses[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "address",
            "addresses": addresses,
            "too_many": too_many,
        }
        return JSONResponse(ctx)
    elif query.endswith(".ans"):
        address = await util.arc0137.get_address_from_domain(db, query)
        if address is None:
            raise HTTPException(status_code=404, detail="ANS domain not found")
        if address == "":
            raise HTTPException(status_code=404, detail="ANS domain is private")
        ctx = {
            "query": query,
            "type": "address",
            "address": address
        }
        return JSONResponse(ctx)
    elif query.startswith("solution1"):
        # solution id
        solutions = await db.search_solution_id(query)
        if not solutions:
            raise HTTPException(status_code=404, detail="Solution not found")
        if len(solutions) == 1:
            return RedirectResponse(f"/solution?id={solutions[0]}{remaining_query}", status_code=302)
        too_many = False
        if len(solutions) > 50:
            solutions = solutions[:50]
            too_many = True
        ctx = {
            "query": query,
            "type": "solution",
            "solutions": solutions,
            "too_many": too_many,
        }
        return JSONResponse(ctx)
    else:
        # have to do this to support program name prefix search
        programs = await db.search_program(query)
        if programs:
            if len(programs) == 1:
                return RedirectResponse(f"/program?id={programs[0]}{remaining_query}", status_code=302)
            too_many = False
            if len(programs) > 50:
                programs = programs[:50]
                too_many = True
            ctx = {
                "query": query,
                "type": "program",
                "programs": programs,
                "too_many": too_many,
            }
            return JSONResponse(ctx)
    raise HTTPException(status_code=404, detail="Unknown object type or searching is not supported")

async def blocks_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 10
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        raise HTTPException(status_code=550, detail="No blocks found")
    if offset < 0 or offset > total_blocks:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = total_blocks - offset
    blocks = await db.get_blocks_range_fast(start, start - limit)

    def get_reward(block: dict[str, Any]):
        block["reward"] = block["block_reward"] + block["coinbase_reward"]  * 2 // 3
        return block
    blocks = [get_reward(block) for block in blocks]
    ctx = {
        "blocks": [format_number(block) for block in blocks],
        "total_count": total_blocks,
    }
    return JSONResponse(ctx)

async def hashrate_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    hashrate_data = await db.get_hashrate(interval[type])
    data: list[dict[str, Any]] = []
    for line in hashrate_data:
        data.append({
            "timestamp": line["timestamp"],
            "hashrate": str(line["hashrate"])
            })
    ctx = {
        "hashrate": data,
    }
    return JSONResponse(ctx)

async def epoch_hashrate_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    epoch_hashrate_data = await db.get_epoch_hashrate(interval[type])
    data: list[dict[str, Any]] = []
    for line in epoch_hashrate_data:
        data.append({
            "height": line["height"],
            "timestamp": line["timestamp"],
            "hashrate": str(line["hashrate"])
            })
    ctx = {
        "epoch_hashrate": data,
    }
    return JSONResponse(ctx)


async def epoch_hash_route(request: Request):
    db: Database = request.app.state.db
    epoch_hashrate_data = await db.get_epoch_hash()
    print(epoch_hashrate_data)
    ctx = {
        "epoch_hash": epoch_hashrate_data,
    }
    return JSONResponse(ctx)

async def coinbase_route(request: Request):
    db: Database = request.app.state.db
    total_blocks = await db.get_latest_height()
    if not total_blocks:
        raise HTTPException(status_code=550, detail="No blocks found")
    coinbases = await db.get_coinbase()
    data: list[dict[str, Any]] = []
    for coinbase in coinbases:
        data.append({
            "height": coinbase["height"],
            "timestamp": coinbase["timestamp"],
            "reward": float(coinbase["reward"])
        })
    ctx = {
        "coinbase": data,
    }
    return JSONResponse(ctx)

async def proof_target_route(request: Request):
    db: Database = request.app.state.db
    type = request.path_params["type"]
    interval = {
        "1d": 86400,
        "7d": 86400 * 7,
        "all": 0
    }
    if type not in interval.keys():
        raise HTTPException(status_code=400, detail="Error trending type")
    proof_targets = await db.get_interval_proof_target(interval[type])
    if not proof_targets:
        raise HTTPException(status_code=550, detail="No blocks found")
    data: list[dict[str, Any]] = []
    for proof_target in proof_targets:
        data.append({
            "height": proof_target["height"],
            "proof_target": int(proof_target["proof_target"]),
            "timestamp": proof_target["timestamp"],
        })
    ctx = {
        "proof_targets": data,
    }
    return JSONResponse(ctx)


async def unconfirmed_transactions_route(request: Request):
    db: Database = request.app.state.db
    try:
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if limit is None:
            limit = 10
        else:
            limit = int(limit)
        if offset is None:
            offset = 0
        else:
            offset = int(offset)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    total_transactions = await db.get_unconfirmed_transaction_count()
    if offset > total_transactions:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = offset
    transactions_data = await db.get_unconfirmed_transactions_range(start, start + limit)
    transactions: list[dict[str, Any]] = []
    for tx in transactions_data:
        transactions.append({
            "tx_id": str(tx.id),
            "type": str(tx.type.name),
            "first_seen": await db.get_transaction_first_seen(str(tx.id)),
        })

    ctx = {
        "transactions": transactions,
        "totalCount": total_transactions,
    }    
    return JSONResponse(ctx)
