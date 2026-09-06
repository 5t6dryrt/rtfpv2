"""
Simple deterministic benchmark for selective recovery.

This does not compare RFTP to HTTP/FTP.
It measures how many bytes must be retransmitted when a known
set of chunks is missing.
"""
import argparse
from pathlib import Path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("file")
    p.add_argument("--chunk-size", type=int, default=65536)
    p.add_argument("--failure-percent", type=float, default=30.0)
    args = p.parse_args()

    size = Path(args.file).stat().st_size
    total_chunks = (size + args.chunk_size - 1) // args.chunk_size
    missing = round(total_chunks * args.failure_percent / 100.0)
    restart_bytes = size
    selective_bytes = min(size, missing * args.chunk_size)

    print(f"File bytes:             {size}")
    print(f"Chunk size:             {args.chunk_size}")
    print(f"Total chunks:           {total_chunks}")
    print(f"Simulated missing:      {missing}")
    print(f"Full restart bytes:     {restart_bytes}")
    print(f"Selective recovery:     {selective_bytes}")
    if restart_bytes:
        saved = 100 * (1 - selective_bytes / restart_bytes)
        print(f"Retransmission savings: {saved:.2f}%")

if __name__ == "__main__":
    main()
