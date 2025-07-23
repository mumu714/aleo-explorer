
from aleo_types import *
from collections import defaultdict

MappingCacheDict = dict[Field, dict[str, Any]]

global_mapping_cache: dict[Field, MappingCacheDict] = {}
global_program_cache: dict[str, dict[int, Program]] = defaultdict(dict)


async def get_program(db: "Database", program_id: str, edition: int) -> Program | None:
    try:
        return global_program_cache[program_id][edition]
    except KeyError:
        program = await db.get_program(program_id, edition)
        if not program:
            return None
        program = Program.load(BytesIO(program))
        global_program_cache[program_id][edition] = program
        return program