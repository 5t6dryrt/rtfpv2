from protocol.header import Header, SIZE


class Packet:
    def __init__(self, header: Header, payload: bytes = b""):
        self.header = header
        self.payload = payload

    def to_bytes(self) -> bytes:
        return self.header.to_bytes() + self.payload

    @classmethod
    def from_bytes(cls, raw: bytes):
        if len(raw) < SIZE:
            raise ValueError(f"Data too short for Header (need {SIZE} bytes, got {len(raw)})")
        header = Header.from_bytes(raw[:SIZE])
        payload = raw[SIZE:SIZE + header.length]
        return cls(header, payload)

    def __repr__(self):
        return f"<Packet type=0x{self.header.pkt_type:02X} status={self.header.status_code} transfer_id={self.header.transfer_id} chunk_id={self.header.chunk_id} len={len(self.payload)}>"