import argparse
import random
import socket
import struct
import threading
import zlib
from pathlib import Path

from protocol.constants import (
    DEFAULT_PORT,
    MAGIC,
    STATUS_PHRASES,
    TYPE_CHUNK,
    TYPE_CHUNK_ACK,
    TYPE_COMPLETE,
    TYPE_CREATE,
    TYPE_CREATE_ACK,
    TYPE_ERROR,
    TYPE_FOUNTAIN_DROP,
    TYPE_FOUNTAIN_INIT,
    TYPE_QUERY,
    TYPE_QUERY_ACK,
    TYPE_VERIFY,
)
from protocol.fountain import FountainDecoder
from protocol.header import Header


class TransferSession:
    def __init__(self, transfer_id: int, filename: str, file_size: int, chunk_size: int, storage_dir: Path):
        self.transfer_id = transfer_id
        self.filename = filename
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.total_chunks = (file_size + chunk_size - 1) // chunk_size if file_size > 0 else 1
        self.storage_dir = storage_dir
        self.temp_file = storage_dir / f"{transfer_id}_{filename}"
        self.received_chunks = set()

        with open(self.temp_file, "wb") as f:
            if file_size > 0:
                f.seek(file_size - 1)
                f.write(b"\0")

    def write_chunk(self, chunk_id: int, data: bytes) -> bool:
        offset = chunk_id * self.chunk_size
        with open(self.temp_file, "r+b") as f:
            f.seek(offset)
            f.write(data)
        self.received_chunks.add(chunk_id)
        return True

    def get_missing_chunks(self) -> list:
        return [i for i in range(self.total_chunks) if i not in self.received_chunks]

    def is_complete(self) -> bool:
        return len(self.received_chunks) >= self.total_chunks


class FountainSession:
    def __init__(self, transfer_id: int, filename: str, file_size: int, chunk_size: int, K: int, storage_dir: Path):
        self.transfer_id = transfer_id
        self.filename = filename
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.K = K
        self.storage_dir = storage_dir
        self.decoder = FountainDecoder(K, file_size, chunk_size)
        self.droplet_count = 0

    def feed_droplet(self, droplet_id: int, data: bytes) -> bool:
        self.droplet_count += 1
        return self.decoder.process_droplet(droplet_id, data)

    def save_file(self):
        dest_file = self.storage_dir / f"{self.transfer_id}_{self.filename}"
        dest_file.write_bytes(self.decoder.get_reconstructed_file())


class RFTPServer:
    def __init__(self, host="0.0.0.0", port=DEFAULT_PORT, storage_dir="storage"):
        self.host = host
        self.port = port
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.sessions = {}
        self.fountain_sessions = {}

    def start(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(5)
        print(f"[*] RFTP v2 Server (Fountain-Ready) listening on {self.host}:{self.port}")

        while True:
            conn, addr = server_sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

    def recv_exact(self, conn, size: int) -> bytes:
        data = b""
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def log_protocol(self, direction: str, op_name: str, status_code: int, transfer_id: int, chunk_id: int):
        phrase = STATUS_PHRASES.get(status_code, "UNKNOWN")
        print(f"[{direction}] Op: {op_name:<18} | Status: {phrase:<24} | Session: {transfer_id} | Index: {chunk_id}")

    def handle_client(self, conn, addr):
        print(f"\n[+] Client connected from {addr}")
        try:
            while True:
                header_bytes = self.recv_exact(conn, Header.SIZE)
                if not header_bytes:
                    break

                header = Header.from_bytes(header_bytes)
                payload = b""
                if header.length > 0:
                    payload = self.recv_exact(conn, header.length)
                    if payload is None:
                        break

                # -------------------------------------------------------------
                # 1. FOUNTAIN MODE INIT
                # -------------------------------------------------------------
                if header.pkt_type == TYPE_FOUNTAIN_INIT:
                    file_size, chunk_size, K = struct.unpack("!QII", payload[:16])
                    filename = payload[16:].decode("utf-8", errors="ignore")
                    transfer_id = random.randint(100000, 999999)

                    self.fountain_sessions[transfer_id] = FountainSession(
                        transfer_id, filename, file_size, chunk_size, K, self.storage_dir
                    )
                    self.log_protocol("RECV", "TYPE_FOUNTAIN_INIT", 200, transfer_id, 0)

                    ack_header = Header(MAGIC, TYPE_CREATE_ACK, 260, transfer_id, 0, 0, 0)
                    conn.sendall(ack_header.to_bytes())
                    self.log_protocol("SEND", "TYPE_CREATE_ACK", 260, transfer_id, 0)

                # -------------------------------------------------------------
                # 2. FOUNTAIN DROPLET STREAM RECEIVE
                # -------------------------------------------------------------
                elif header.pkt_type == TYPE_FOUNTAIN_DROP:
                    f_session = self.fountain_sessions.get(header.transfer_id)
                    if not f_session:
                        continue

                    # ถ้าถอดรหัสครบ 100% แล้ว ให้ข้าม Droplets ส่วนเกินที่ค้างใน Buffer
                    if len(f_session.decoder.solved) == f_session.K:
                        continue

                    droplet_id = header.chunk_id
                    is_complete = f_session.feed_droplet(droplet_id, payload)
                    solved_now = len(f_session.decoder.solved)

                    print(f"\r[⛲ FOUNTAIN DECODER] Droplets: {f_session.droplet_count} | Solved Chunks: [{solved_now}/{f_session.K}]", end="", flush=True)

                    if is_complete:
                        print(f"\n[✓] [GAUSSIAN SOLVER] 100% OF CHUNKS DECODED! Reconstructing file...")
                        f_session.save_file()
                        comp_header = Header(MAGIC, TYPE_COMPLETE, 200, header.transfer_id, 0, 0, 0)
                        conn.sendall(comp_header.to_bytes())
                        self.log_protocol("SEND", "TYPE_COMPLETE", 200, header.transfer_id, 0)
                        # ไม่ใส่ break เพื่อให้ Client อ่าน TYPE_COMPLETE แล้วเป็นฝ่ายปิด Socket เอง
                # -------------------------------------------------------------
                # 3. STANDARD TRANSFER (โหมดปกติเดิม)
                # -------------------------------------------------------------
                elif header.pkt_type == TYPE_CREATE:
                    file_size, chunk_size = struct.unpack("!QQ", payload[:16])
                    filename = payload[16:].decode("utf-8", errors="ignore")
                    transfer_id = random.randint(100000, 999999)

                    self.sessions[transfer_id] = TransferSession(
                        transfer_id, filename, file_size, chunk_size, self.storage_dir
                    )
                    self.log_protocol("RECV", "TYPE_CREATE", 200, transfer_id, 0)

                    ack_header = Header(MAGIC, TYPE_CREATE_ACK, 201, transfer_id, 0, 0, 0)
                    conn.sendall(ack_header.to_bytes())
                    self.log_protocol("SEND", "TYPE_CREATE_ACK", 201, transfer_id, 0)

                elif header.pkt_type == TYPE_QUERY:
                    session = self.sessions.get(header.transfer_id)
                    if not session:
                        err_header = Header(MAGIC, TYPE_ERROR, 404, header.transfer_id, 0, 0, 0)
                        conn.sendall(err_header.to_bytes())
                        continue

                    missing = session.get_missing_chunks()
                    resp_payload = struct.pack("!II", session.total_chunks, len(missing))
                    for m_id in missing:
                        resp_payload += struct.pack("!I", m_id)

                    ack_header = Header(MAGIC, TYPE_QUERY_ACK, 202, header.transfer_id, 0, len(resp_payload), 0)
                    conn.sendall(ack_header.to_bytes() + resp_payload)

                elif header.pkt_type == TYPE_CHUNK:
                    session = self.sessions.get(header.transfer_id)
                    if not session:
                        continue
                    session.write_chunk(header.chunk_id, payload)
                    ack_header = Header(MAGIC, TYPE_CHUNK_ACK, 206, header.transfer_id, header.chunk_id, 0, 0)
                    conn.sendall(ack_header.to_bytes())

                elif header.pkt_type == TYPE_VERIFY:
                    session = self.sessions.get(header.transfer_id)
                    if session and session.is_complete():
                        comp_header = Header(MAGIC, TYPE_COMPLETE, 200, header.transfer_id, 0, 0, 0)
                        conn.sendall(comp_header.to_bytes())

        except Exception as e:
            print(f"[!] Server Error: {e}")
        finally:
            conn.close()
            print(f"[-] Client {addr} disconnected.")


def main():
    parser = argparse.ArgumentParser(description="RFTP v2 Server with Fountain Protocol")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
    parser.add_argument("--storage", default="storage", help="Storage directory")
    args = parser.parse_args()

    RFTPServer(port=args.port, storage_dir=args.storage).start()


if __name__ == "__main__":
    main()