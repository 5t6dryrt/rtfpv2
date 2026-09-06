import struct

# Magic(2B), Type(1B), StatusCode(2B), TransferID(4B), ChunkID(4B), Length(4B), Checksum(4B)
HEADER_FORMAT = "!H B H I I I I"
SIZE = struct.calcsize(HEADER_FORMAT)
HEADER_SIZE = SIZE


class Header:
    FORMAT = HEADER_FORMAT
    SIZE = SIZE

    def __init__(self, magic: int, pkt_type: int, status_code: int, transfer_id: int, chunk_id: int, length: int, checksum: int = 0):
        self.magic = magic
        self.pkt_type = pkt_type
        self.status_code = status_code
        self.transfer_id = transfer_id
        self.chunk_id = chunk_id
        self.length = length
        self.checksum = checksum

    def to_bytes(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.magic,
            self.pkt_type,
            self.status_code,
            self.transfer_id,
            self.chunk_id,
            self.length,
            self.checksum
        )

    @classmethod
    def from_bytes(cls, raw: bytes):
        if len(raw) < cls.SIZE:
            raise ValueError(f"Header raw data too short (need {cls.SIZE} bytes, got {len(raw)})")
        magic, pkt_type, status_code, transfer_id, chunk_id, length, checksum = struct.unpack(cls.FORMAT, raw[:cls.SIZE])
        return cls(magic, pkt_type, status_code, transfer_id, chunk_id, length, checksum)