from protocol.bitmap import ChunkBitmap
from protocol.constants import MAX_CHUNK_SIZE

class Transfer:
    def __init__(self, transfer_id, filename, file_size, chunk_size):
        if chunk_size <= 0 or chunk_size > MAX_CHUNK_SIZE:
            raise ValueError("Invalid chunk size")
        self.transfer_id = transfer_id
        self.filename = filename
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.total_chunks = (file_size + chunk_size - 1) // chunk_size
        self.bitmap = ChunkBitmap(self.total_chunks)
        self.state = "CREATED"

    def to_meta(self):
        return {
            "filename": self.filename,
            "file_size": self.file_size,
            "chunk_size": self.chunk_size,
            "total_chunks": self.total_chunks,
            "bitmap": self.bitmap.to_bytes().hex(),
            "state": self.state,
        }

    @classmethod
    def from_meta(cls, transfer_id, meta):
        t = cls(transfer_id, meta["filename"], meta["file_size"], meta["chunk_size"])
        t.bitmap = ChunkBitmap.from_bytes(t.total_chunks, bytes.fromhex(meta["bitmap"]))
        t.state = meta.get("state", "CREATED")
        return t
