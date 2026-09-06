import argparse
import hashlib
import random
import select
import socket
import struct
import sys
import time
import zlib
from pathlib import Path

from protocol.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_PORT,
    MAGIC,
    STATUS_PHRASES,
    TYPE_CHUNK,
    TYPE_CHUNK_ACK,
    TYPE_COMPLETE,
    TYPE_CREATE,
    TYPE_CREATE_ACK,
    TYPE_FOUNTAIN_DROP,
    TYPE_FOUNTAIN_INIT,
    TYPE_QUERY,
    TYPE_QUERY_ACK,
    TYPE_VERIFY,
)
from protocol.fountain import FountainEncoder
from protocol.header import Header
from protocol.packet import Packet


class RFTPClient:
    def __init__(self, host="127.0.0.1", port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def log_protocol(self, direction: str, op_name: str, status_code: int, transfer_id: int, chunk_id: int):
        phrase = STATUS_PHRASES.get(status_code, "UNKNOWN")
        print(f"[{direction}] Op: {op_name:<18} | Status: {phrase:<24} | Session: {transfer_id} | Index: {chunk_id}")

    def send_packet(self, pkt: Packet):
        self.sock.sendall(pkt.to_bytes())

    def recv_packet(self) -> Packet:
        header_bytes = b""
        while len(header_bytes) < Header.SIZE:
            chunk = self.sock.recv(Header.SIZE - len(header_bytes))
            if not chunk:
                raise ConnectionError("Connection closed while receiving header.")
            header_bytes += chunk

        header = Header.from_bytes(header_bytes)
        payload = b""
        while len(payload) < header.length:
            chunk = self.sock.recv(header.length - len(payload))
            if not chunk:
                raise ConnectionError("Connection lost while reading payload.")
            payload += chunk

        return Packet(header, payload)

    # =========================================================================
    # 1. STANDARD & RESUMABLE TRANSFER MODE
    # =========================================================================
    def upload_standard(self, file_path: str, chunk_size: int = DEFAULT_CHUNK_SIZE, interrupt_after: int = None, resume_id: int = None):
        path = Path(file_path)
        if not path.exists():
            print(f"[!] File not found: {file_path}")
            return

        file_data = path.read_bytes()
        file_size = len(file_data)
        file_sha256 = hashlib.sha256(file_data).hexdigest()
        total_chunks = (file_size + chunk_size - 1) // chunk_size if file_size > 0 else 1

        self.connect()
        start_time = time.perf_counter()

        if resume_id:
            transfer_id = resume_id
            print(f"\n[*] Requesting State Recovery for Session ID: {transfer_id}")
            q_header = Header(MAGIC, TYPE_QUERY, 200, transfer_id, 0, 0, 0)
            self.send_packet(Packet(q_header, b""))
            self.log_protocol("SEND", "TYPE_QUERY", 200, transfer_id, 0)

            resp = self.recv_packet()
            self.log_protocol("RECV", "TYPE_QUERY_ACK", resp.header.status_code, transfer_id, 0)

            if resp.header.pkt_type != TYPE_QUERY_ACK:
                print(f"[!] Session {transfer_id} not found on server.")
                self.close()
                return

            tot_c, missing_count = struct.unpack("!II", resp.payload[:8])
            missing_chunks = [
                struct.unpack("!I", resp.payload[8 + i * 4 : 12 + i * 4])[0]
                for i in range(missing_count)
            ]
            print(f"[+] Server State: {len(missing_chunks)}/{tot_c} Chunks Missing -> {missing_chunks}")
            chunks_to_send = missing_chunks
        else:
            filename_bytes = path.name.encode("utf-8")
            payload = struct.pack("!QQ", file_size, chunk_size) + filename_bytes
            chk = zlib.crc32(payload) & 0xFFFFFFFF
            header = Header(MAGIC, TYPE_CREATE, 200, 0, 0, len(payload), chk)
            self.send_packet(Packet(header, payload))
            self.log_protocol("SEND", "TYPE_CREATE", 200, 0, 0)

            resp = self.recv_packet()
            transfer_id = resp.header.transfer_id
            self.log_protocol("RECV", "TYPE_CREATE_ACK", resp.header.status_code, transfer_id, 0)
            chunks_to_send = list(range(total_chunks))

        bytes_sent = 0
        with open(path, "rb") as f:
            for chunk_id in chunks_to_send:
                f.seek(chunk_id * chunk_size)
                chunk_bytes = f.read(chunk_size)
                chunk_chk = zlib.crc32(chunk_bytes) & 0xFFFFFFFF

                header = Header(MAGIC, TYPE_CHUNK, 200, transfer_id, chunk_id, len(chunk_bytes), chunk_chk)
                self.send_packet(Packet(header, chunk_bytes))
                self.log_protocol("SEND", "TYPE_CHUNK", 200, transfer_id, chunk_id)

                ack = self.recv_packet()
                self.log_protocol("RECV", "TYPE_CHUNK_ACK", ack.header.status_code, transfer_id, chunk_id)

                bytes_sent += len(chunk_bytes)
                if interrupt_after and bytes_sent >= interrupt_after:
                    print(f"\n[!] SIMULATED CONNECTION LOSS at {bytes_sent} bytes!")
                    print(f"[*] To resume this transfer, execute:")
                    print(f"    python -m client.client {file_path} --resume {transfer_id}\n")
                    self.close()
                    return

        # Verification
        v_header = Header(MAGIC, TYPE_VERIFY, 200, transfer_id, 0, 0, 0)
        self.send_packet(Packet(v_header, b""))
        self.log_protocol("SEND", "TYPE_VERIFY", 200, transfer_id, 0)

        final_resp = self.recv_packet()
        self.log_protocol("RECV", "TYPE_COMPLETE", final_resp.header.status_code, transfer_id, 0)
        elapsed = time.perf_counter() - start_time

        if final_resp.header.pkt_type == TYPE_COMPLETE:
            throughput_mb = (bytes_sent / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            print("\n" + "=" * 65)
            print("✓ [TRANSFER SUMMARY & INTEGRITY REPORT]")
            print("=" * 65)
            print(f"  • Transfer Session ID: #{transfer_id}")
            print(f"  • Total Transferred:   {bytes_sent:,} Bytes")
            print(f"  • Time Elapsed:        {elapsed:.3f} s ({throughput_mb:.2f} MB/s)")
            print(f"  • SHA-256 Digest:      {file_sha256}")
            print(f"  • Status:              200 OK (Verified Complete)")
            print("=" * 65 + "\n")

        self.close()

    # =========================================================================
    # 2. DIGITAL FOUNTAIN STREAMING MODE
    # =========================================================================
    def upload_fountain(self, file_path: str, chunk_size: int = 1024, loss_rate: float = 0.3):
        path = Path(file_path)
        if not path.exists():
            print(f"[!] File not found: {file_path}")
            return

        file_data = path.read_bytes()
        file_size = len(file_data)
        file_sha256 = hashlib.sha256(file_data).hexdigest()

        encoder = FountainEncoder(file_data, chunk_size=chunk_size)
        self.connect()

        print("\n" + "=" * 65)
        print("⛲ [RFTP v2: DIGITAL FOUNTAIN ZERO-RETRANSMISSION STREAMING]")
        print("=" * 65)
        print(f"[*] Payload: {path.name} ({file_size:,} bytes)")
        print(f"[*] Partition: {encoder.K} Source Chunks (Chunk Size: {chunk_size} B)")
        print(f"[*] In-Flight Packet Loss Rate: {loss_rate * 100:.1f}%")
        print("-" * 65)

        start_time = time.perf_counter()

        filename_bytes = path.name.encode("utf-8")
        payload = struct.pack("!QII", file_size, chunk_size, encoder.K) + filename_bytes
        header = Header(MAGIC, TYPE_FOUNTAIN_INIT, 200, 0, 0, len(payload), 0)
        self.send_packet(Packet(header, payload))
        self.log_protocol("SEND", "TYPE_FOUNTAIN_INIT", 200, 0, 0)

        resp = self.recv_packet()
        transfer_id = resp.header.transfer_id
        self.log_protocol("RECV", "TYPE_CREATE_ACK", resp.header.status_code, transfer_id, 0)

        droplet_id = 0
        droplets_sent = 0
        droplets_lost = 0

        self.sock.setblocking(False)

        try:
            while True:
                readable, _, _ = select.select([self.sock], [], [], 0.0)
                if readable:
                    self.sock.setblocking(True)
                    final_resp = self.recv_packet()
                    if final_resp.header.pkt_type == TYPE_COMPLETE:
                        elapsed = time.perf_counter() - start_time
                        print("\n\n" + "=" * 65)
                        print("🎉 [FOUNTAIN RECONSTRUCTION COMPLETED]")
                        print("=" * 65)
                        print(f"  • Transfer Session ID: #{transfer_id}")
                        print(f"  • Source Chunks (K):    {encoder.K}")
                        print(f"  • Droplets Generated:   {droplet_id}")
                        print(f"  • Droplets Delivered:   {droplets_sent}")
                        print(f"  • Droplets Dropped:     {droplets_lost} ({loss_rate * 100:.1f}%)")
                        print(f"  • Retransmissions:      0 (Zero-Retransmission Architecture)")
                        print(f"  • Decoding Time:        {elapsed:.3f} s")
                        print(f"  • SHA-256 Digest:       {file_sha256}")
                        print(f"  • Final Status:         200 OK")
                        print("=" * 65 + "\n")
                        break
                    self.sock.setblocking(False)

                droplet_id += 1

                if random.random() < loss_rate:
                    droplets_lost += 1
                    continue

                droplet_bytes = encoder.generate_droplet(droplet_id)
                d_header = Header(MAGIC, TYPE_FOUNTAIN_DROP, 200, transfer_id, droplet_id, len(droplet_bytes), 0)

                try:
                    self.send_packet(Packet(d_header, droplet_bytes))
                    droplets_sent += 1
                except (BlockingIOError, socket.error):
                    time.sleep(0.002)
                    continue

                print(f"\r  -> Streaming Droplet #{droplet_id} (Sent: {droplets_sent} | Dropped: {droplets_lost})", end="", flush=True)
                time.sleep(0.001)

        finally:
            self.close()


def main():
    parser = argparse.ArgumentParser(description="RFTP v2 Client")
    parser.add_argument("file", help="File to upload")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Chunk size")
    parser.add_argument("--interrupt-after", type=int, default=None, help="Abort after N bytes sent")
    parser.add_argument("--resume", type=int, default=None, help="Resume Transfer ID")
    parser.add_argument("--fountain", action="store_true", help="Enable Digital Fountain Rateless Streaming Mode")
    parser.add_argument("--loss-rate", type=float, default=0.3, help="Simulate Packet Loss rate (e.g. 0.3 = 30%%)")

    args = parser.parse_args()
    client = RFTPClient(args.host, args.port)

    if args.fountain:
        f_chunk = min(args.chunk_size, 1024) if args.chunk_size == DEFAULT_CHUNK_SIZE else args.chunk_size
        client.upload_fountain(args.file, chunk_size=f_chunk, loss_rate=args.loss_rate)
    else:
        client.upload_standard(args.file, args.chunk_size, args.interrupt_after, args.resume)


if __name__ == "__main__":
    main()