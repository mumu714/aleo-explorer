from aleo_types import Value, Address


class Registers:

    def __init__(self):
        self._registers: dict[int, Value] = {}
        self.owner: Address | None = None

    def __getitem__(self, index: int):
        if index not in self._registers:
            raise IndexError(index)
        return self._registers[index]

    def __setitem__(self, index: int, value: Value):
        self._registers[index] = value

    def dump(self):
        last_i, last_r = None, None
        for i, r in self._registers.items():
            last_i, last_r = i, r 
        if last_i is not None and last_r is not None:
            print(f"r{last_i} = {last_r}")
