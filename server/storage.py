import json
from pathlib import Path

class TransferStorage:
    def __init__(self, root="received"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / ".meta").mkdir(exist_ok=True)

    def meta_path(self, transfer_id):
        return self.root / ".meta" / f"{transfer_id}.json"

    def part_path(self, transfer_id):
        return self.root / f"{transfer_id}.part"

    def save_meta(self, transfer_id, meta):
        self.meta_path(transfer_id).write_text(json.dumps(meta, indent=2))

    def load_meta(self, transfer_id):
        p = self.meta_path(transfer_id)
        if not p.exists():
            return None
        return json.loads(p.read_text())

    def write_chunk(self, transfer_id, chunk_id, data, chunk_size):
        path = self.part_path(transfer_id)
        mode = "r+b" if path.exists() else "wb"
        with path.open(mode) as f:
            f.seek(chunk_id * chunk_size)
            f.write(data)

    def finalize(self, transfer_id, filename):
        src = self.part_path(transfer_id)
        safe_name = Path(filename).name
        dst = self.root / safe_name
        src.replace(dst)
        mp = self.meta_path(transfer_id)
        if mp.exists():
            mp.unlink()
        return dst
