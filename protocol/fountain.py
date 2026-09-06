import bisect
import math
import random
from collections import deque
from typing import Dict, List, Set, Tuple


# =========================================================================
# 1. ROBUST SOLITON DISTRIBUTION (RSD) GENERATOR
# =========================================================================
class RobustSolitonDistribution:
    """
    คำนวณและแคช Cumulative Distribution Function (CDF) ของ Robust Soliton
    ช่วยควบคุม Ripple Size ในกระบวนการ Belief Propagation ไม่ให้เป็น 0 ก่อนถอดรหัสเสร็จ
    """
    _cdf_cache: Dict[int, List[float]] = {}

    @classmethod
    def get_cdf(cls, K: int, c: float = 0.1, delta: float = 0.05) -> List[float]:
        if K in cls._cdf_cache:
            return cls._cdf_cache[K]

        if K == 1:
            cls._cdf_cache[K] = [1.0]
            return [1.0]

        # 1. Ideal Soliton Distribution: rho(d)
        rho = [0.0] * (K + 1)
        rho[1] = 1.0 / K
        for d in range(2, K + 1):
            rho[d] = 1.0 / (d * (d - 1))

        # 2. Robust Tuning Function: tau(d)
        R = c * math.log(K / delta) * math.sqrt(K)
        pivot = max(1, min(int(math.floor(K / R)), K))

        tau = [0.0] * (K + 1)
        for d in range(1, pivot):
            tau[d] = R / (d * K)
        if pivot <= K:
            tau[pivot] = (R * math.log(R / delta)) / K

        # 3. Robust Soliton: mu(d) = (rho(d) + tau(d)) / beta
        mu = [rho[d] + tau[d] for d in range(K + 1)]
        beta = sum(mu)
        mu = [val / beta for val in mu]

        # 4. สร้าง Cumulative Distribution Function (CDF) สำหรับ Binary Search
        cdf = []
        running_sum = 0.0
        for d in range(1, K + 1):
            running_sum += mu[d]
            cdf.append(running_sum)
        cdf[-1] = 1.0  # ป้องกัน Floating-point precision error

        cls._cdf_cache[K] = cdf
        return cdf

    @classmethod
    def sample_degree(cls, rng: random.Random, K: int) -> int:
        if K <= 1:
            return 1
        cdf = cls.get_cdf(K)
        p = rng.random()
        degree = bisect.bisect_left(cdf, p) + 1
        return min(max(1, degree), K)


def get_droplet_indices(seed: int, K: int) -> List[int]:
    """สุ่ม Degree ตาม RSD และเลือก Chunk Indices อย่างคงที่ตามค่า Seed"""
    rng = random.Random(seed)
    degree = RobustSolitonDistribution.sample_degree(rng, K)
    return sorted(rng.sample(range(K), degree))


# =========================================================================
# 2. OPTIMIZED FOUNTAIN ENCODER
# =========================================================================
class FountainEncoder:
    def __init__(self, data: bytes, chunk_size: int = 1024):
        self.chunk_size = chunk_size
        self.original_len = len(data)

        self.chunks = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            if len(chunk) < chunk_size:
                chunk = chunk.ljust(chunk_size, b"\0")
            self.chunks.append(chunk)

        self.K = max(1, len(self.chunks))
        # Precompute CDF รอไว้ตั้งแต่เริ่มต้น
        RobustSolitonDistribution.get_cdf(self.K)

    def generate_droplet(self, droplet_id: int) -> bytes:
        indices = get_droplet_indices(droplet_id, self.K)
        mixed = bytearray(self.chunk_size)
        for idx in indices:
            c = self.chunks[idx]
            for b in range(self.chunk_size):
                mixed[b] ^= c[b]
        return bytes(mixed)


# =========================================================================
# 3. HIGH-SPEED RIPPLE SOLVER (INVERTED INDEX + QUEUE)
# =========================================================================
class FountainDecoder:
    """
    แก้สมการแบบ Belief Propagation โดยใช้ Inverted Index:
    ลดเวลาค้นหาสมการจาก O(Equations) เป็น O(1) ต่อการ Propagation 1 ครั้ง
    """
    def __init__(self, K: int, original_len: int, chunk_size: int = 1024):
        self.K = K
        self.original_len = original_len
        self.chunk_size = chunk_size
        self.solved: Dict[int, bytes] = {}
        
        # Inverted index: chunk_id -> list of equation_ids ที่มี chunk นั้นอยู่
        self.equations: Dict[int, Tuple[Set[int], bytearray]] = {}
        self.chunk_to_eqs: Dict[int, Set[int]] = {i: set() for i in range(K)}
        self.eq_counter = 0

    def process_droplet(self, droplet_id: int, data: bytes) -> bool:
        if len(self.solved) == self.K:
            return True

        indices = set(get_droplet_indices(droplet_id, self.K))
        curr_data = bytearray(data)

        # 1. หักล้าง Chunks ที่เคยแก้เสร็จไปแล้วออกจาก Droplet ใหม่
        for idx in list(indices):
            if idx in self.solved:
                indices.remove(idx)
                solved_bytes = self.solved[idx]
                for b in range(self.chunk_size):
                    curr_data[b] ^= solved_bytes[b]

        if not indices:
            return len(self.solved) == self.K

        # 2. เริ่มกระบวนการ Ripple Solving ด้วย Queue
        ripple_queue = deque()

        if len(indices) == 1:
            solved_idx = indices.pop()
            self.solved[solved_idx] = bytes(curr_data)
            ripple_queue.append(solved_idx)
        else:
            eq_id = self.eq_counter
            self.eq_counter += 1
            self.equations[eq_id] = (indices, curr_data)
            for idx in indices:
                self.chunk_to_eqs[idx].add(eq_id)

        # 3. ส่งต่อผลลัพธ์แบบลูกโซ่ (Ripple Propagation)
        while ripple_queue:
            curr_solved_idx = ripple_queue.popleft()
            curr_solved_bytes = self.solved[curr_solved_idx]

            # ดึงเฉพาะสมการที่มี curr_solved_idx อยู่ (O(1) lookup)
            affected_eq_ids = list(self.chunk_to_eqs[curr_solved_idx])
            self.chunk_to_eqs[curr_solved_idx].clear()

            for eq_id in affected_eq_ids:
                if eq_id not in self.equations:
                    continue

                eq_indices, eq_data = self.equations[eq_id]
                if curr_solved_idx in eq_indices:
                    eq_indices.remove(curr_solved_idx)
                    for b in range(self.chunk_size):
                        eq_data[b] ^= curr_solved_bytes[b]

                    # หากสมการเหลือตัวแปรเดียว ถอดรหัสได้ทันที
                    if len(eq_indices) == 1:
                        new_solved_idx = eq_indices.pop()
                        self.solved[new_solved_idx] = bytes(eq_data)
                        del self.equations[eq_id]
                        ripple_queue.append(new_solved_idx)
                    elif len(eq_indices) == 0:
                        del self.equations[eq_id]

        return len(self.solved) == self.K

    def get_reconstructed_file(self) -> bytes:
        full = bytearray()
        for i in range(self.K):
            if i in self.solved:
                full.extend(self.solved[i])
            else:
                full.extend(b"\0" * self.chunk_size)
        return bytes(full[: self.original_len])