from io import BytesIO

from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import Program, Value, LiteralPlaintextType, LiteralPlaintext, \
    Literal, StructPlaintextType, StructPlaintext, cached_get_key_id
from api.utils import async_check_sync, use_program_cache
from db import Database


@async_check_sync
@use_program_cache
async def mapping_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    _ = request.path_params["version"]
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    key = request.path_params["key"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=200)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    if mapping not in program.mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=200)
    map_key_type = program.mappings[mapping].key.plaintext_type
    if isinstance(map_key_type, LiteralPlaintextType):
        primitive_type = map_key_type.literal_type.primitive_type
        try:
            key = primitive_type.loads(key)
        except:
            return JSONResponse({"error": "Invalid key"}, status_code=200)
        key = LiteralPlaintext(literal=Literal(type_=Literal.reverse_primitive_type_map[primitive_type], primitive=key))
    elif isinstance(map_key_type, StructPlaintextType):
        structs = program.structs
        struct_type = structs[map_key_type.struct]
        try:
            value = StructPlaintext.loads(key, struct_type, structs)
        except Exception as e:
            return JSONResponse({"error": f"Invalid struct key: {e} (experimental feature, if you believe this is an error please submit a feedback)"}, status_code=200)
        key = value
    else:
        return JSONResponse({"error": "Unknown key type"}, status_code=200)
    key_id = cached_get_key_id(program_id, mapping, key.dump())
    value = await db.get_mapping_value(program_id, mapping, key_id)
    if value is None:
        return JSONResponse(None)
    return JSONResponse(str(Value.load(BytesIO(value))))

@async_check_sync
@use_program_cache
async def mapping_list_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    _ = request.path_params["version"]
    program_id = request.path_params["program_id"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    return JSONResponse(list(map(str, mappings.keys())))

@async_check_sync
@use_program_cache
async def mapping_value_list_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    if mapping not in mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)

    if version <= 1:
        mapping_cache = await db.get_mapping_cache(program_id, mapping)
        res: dict[str, dict[str, str]] = {}
        for key_id, item in mapping_cache.items():
            res[str(key_id)] = {
                "key": str(item["key"]),
                "value": str(item["value"]),
            }
        return JSONResponse(res)

    else:
        count = int(request.query_params.get("count", 50))
        if count > 100:
            count = 100
        cursor = int(request.query_params.get("cursor", 0))
        mapping_data = await db.get_mapping_key_value(program_id, mapping, count, cursor)
        res: list[dict[str, str]] = []
        for key_id, item in mapping_data[0].items():
            res.append({
                "key": str(item["key"]),
                "value": str(item["value"]),
            })

        return JSONResponse({"result": res, "cursor": mapping_data[1]})

@async_check_sync
@use_program_cache
async def mapping_key_count_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version <= 1:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    program_id = request.path_params["program_id"]
    mapping = request.path_params["mapping"]
    try:
        program = program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return JSONResponse({"error": "Program not found"}, status_code=404)
        program = Program.load(BytesIO(program))
        program_cache[program_id] = program
    mappings = program.mappings
    if mapping not in mappings:
        return JSONResponse({"error": "Mapping not found"}, status_code=404)
    return JSONResponse(await db.get_mapping_key_count(program_id, mapping))