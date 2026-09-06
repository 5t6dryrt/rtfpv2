import unittest
from protocol.bitmap import ChunkBitmap
from protocol.header import Header
from protocol.packet import build_packet, parse_packet
from protocol.constants import CHUNK

class ProtocolTests(unittest.TestCase):
    def test_header_roundtrip(self):
        h = Header(CHUNK, 0, 1, 2, 3, 4, 123)
        self.assertEqual(Header.unpack(h.pack()), h)

    def test_packet_roundtrip(self):
        payload = b"hello"
        packet = build_packet(CHUNK, 1, 2, 3, payload)
        h, p = parse_packet(packet[:24], packet[24:])
        self.assertEqual(p, payload)
        self.assertEqual(h.chunk_id, 3)

    def test_bitmap(self):
        b = ChunkBitmap(10)
        b.mark_received(2)
        b.mark_received(7)
        self.assertEqual(b.missing(), [0,1,3,4,5,6,8,9])
        restored = ChunkBitmap.from_bytes(10, b.to_bytes())
        self.assertTrue(restored.is_received(7))

if __name__ == "__main__":
    unittest.main()
