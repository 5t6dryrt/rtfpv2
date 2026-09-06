class ChunkBitmap:
    def __init__(self, total_chunks: int):
        if total_chunks < 0:
            raise ValueError("total_chunks must be non-negative")
        self.total_chunks = total_chunks
        self.bits = bytearray((total_chunks + 7) // 8)

    def mark_received(self, chunk_id: int):
        self._check(chunk_id)
        self.bits[chunk_id // 8] |= 1 << (7 - (chunk_id % 8))

    def is_received(self, chunk_id: int) -> bool:
        self._check(chunk_id)
        return bool(self.bits[chunk_id // 8] & (1 << (7 - (chunk_id % 8))))

    def missing(self):
        return [i for i in range(self.total_chunks) if not self.is_received(i)]

    def received_count(self):
        return self.total_chunks - len(self.missing())

    def to_bytes(self):
        return bytes(self.bits)

    @classmethod
    def from_bytes(cls, total_chunks, data):
        obj = cls(total_chunks)
        if len(data) != len(obj.bits):
            raise ValueError("Invalid bitmap length")
        obj.bits[:] = data
        return obj

    def _check(self, chunk_id):
        if not 0 <= chunk_id < self.total_chunks:
            raise IndexError(f"Invalid chunk id: {chunk_id}")
