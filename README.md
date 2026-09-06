# RFTP v2 — Selective Recovery File Transfer Protocol

Educational TCP application-layer protocol for:
- chunk-level transfer
- per-packet CRC32 integrity
- explicit transfer state
- QUERY-based chunk bitmap recovery
- selective retransmission
- multi-client threaded server

## Requirements
Python 3.10+ recommended. No third-party packages.

## Run

From the `rftp_v2` directory:

```bash
python -m server.server --port 9000
```

Create a test file:

```bash
python -c "from pathlib import Path; Path('test.bin').write_bytes(b'A'*1000000)"
```

Upload:

```bash
python -m client.client test.bin
```

## Simulate interruption

```bash
python -m client.client test.bin --interrupt-after 300000
```

The current MVP demonstrates interruption and persistent server-side chunk state.
The selective recovery client workflow should be completed as the next integration step:
reconnect -> QUERY -> parse bitmap -> RESUME -> send missing chunks.

## Tests

```bash
python -m unittest discover -s tests
```

## Benchmark

```bash
python -m benchmark.benchmark test.bin --failure-percent 30
```

## Wireshark

Capture TCP traffic on port 9000:

```text
tcp.port == 9000
```

RFTP uses a fixed 24-byte binary application header followed by payload.
