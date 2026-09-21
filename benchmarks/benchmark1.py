import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.core import MCLoop



# Small CPU-bound workload (~0.2 to 0.5 ms per run)
def cpu_microtask(n: int = 150_000) -> int:
    acc = 0
    for i in range(n):
        acc = (acc + i * 3) ^ (i & 0xFF)
    return acc

NUM_JOBS = 5000
WORKERS = 4

def run_single_thread():
    print(f"-> Running {NUM_JOBS} tasks sequentially on 1 core...")
    t0 = time.perf_counter()
    for _ in range(NUM_JOBS):
        cpu_microtask()
    elapsed = time.perf_counter() - t0
    print(f"   Sequential Time: {elapsed:.3f}s")
    return elapsed

def run_standard_threads():
    print(f"-> Running {NUM_JOBS} tasks with ThreadPoolExecutor ({WORKERS} threads, shared GIL)...")
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        # submit all jobs and drain results
        results = list(executor.map(lambda _: cpu_microtask(), range(NUM_JOBS)))
    elapsed = time.perf_counter() - t0
    print(f"   Standard Threads Time: {elapsed:.3f}s")
    return elapsed

def run_mcloop():
    print(f"-> Running {NUM_JOBS} tasks with MCLoop ({WORKERS} subinterpreters, per-interpreter GIL)...")
    loop = MCLoop(no_of_interpreters=WORKERS, total_memory=200 * 1024 * 1024)
    t0 = time.perf_counter()
    
    # Fire off all jobs
    job_ids = [loop.submit(cpu_microtask) for _ in range(NUM_JOBS)]
    
    # Wait for all jobs to complete
    for j_id in job_ids:
        loop.fetch(j_id)
        
    elapsed = time.perf_counter() - t0
    loop.close()
    print(f"   MCLoop Time: {elapsed:.3f}s")
    return elapsed

if __name__ == "__main__":
    print(f"--- Starting Multicore Benchmark: {NUM_JOBS} tasks across {WORKERS} workers ---\n")
    
    t_seq = run_single_thread()
    t_thr = run_standard_threads()
    t_mc  = run_mcloop()

    print("\n--- Summary & Speedup vs Single-Thread ---")
    print(f"Sequential (1 core)       : {t_seq:.3f}s  (1.00x baseline)")
    print(f"Standard Threading (GIL)  : {t_thr:.3f}s  ({t_seq / t_thr:.2f}x)")
    print(f"MCLoop (Subinterpreters)  : {t_mc:.3f}s   ({t_seq / t_mc:.2f}x)")