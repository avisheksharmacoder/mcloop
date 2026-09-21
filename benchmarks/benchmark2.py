import os
import time
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import sys
from pathlib import Path

system_root = Path(__file__).resolve().parents[1]
sys.path.append(str(system_root))

from src.core import MCLoop

def get_process_rss_mb() -> float:
    """Reads the exact Resident Set Size (physical memory) of the current process."""
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0  # Convert kB to MB
    except Exception:
        return 0.0

# Microtask: fast CPU burst (~0.5ms)
def cpu_workload(n: int = 150_000) -> int:
    acc = 0
    for i in range(n):
        acc = (acc + i * 3) ^ (i & 0xFF)
    return acc

NUM_TASKS = 5_000
WORKERS = 4


def benchmark_mcloop():
    print(f"\n--- [1] Testing MCLoop ({WORKERS} Subinterpreters) ---")
    rss_before = get_process_rss_mb()
    t_start = time.perf_counter()
    
    loop = MCLoop(no_of_interpreters=WORKERS, total_memory=200 * 1024 * 1024)
    t_init = time.perf_counter() - t_start
    
    # Measure memory directly after spinning up workers
    rss_after_init = get_process_rss_mb()
    
    t_exec_start = time.perf_counter()
    job_ids = [loop.submit(cpu_workload) for _ in range(NUM_TASKS)]
    for j_id in job_ids:
        loop.fetch(j_id)
        
    t_total = time.perf_counter() - t_start
    t_compute = time.perf_counter() - t_exec_start
    
    loop.close()
    
    mem_overhead = max(0.0, rss_after_init - rss_before)
    print(f"  * Init Time       : {t_init * 1000:.2f} ms")
    print(f"  * Compute Time    : {t_compute:.3f} s")
    print(f"  * Total Wall-Clock: {t_total:.3f} s")
    print(f"  * Worker RAM Delta: ~{mem_overhead:.2f} MB (Single Process)")
    return t_init, t_compute, t_total, mem_overhead


def get_total_tree_rss_mb() -> float:
    """Calculates RSS for the parent process AND all spawned child processes."""
    total_rss_kb = 0.0
    
    # 1. Read Parent VmRSS
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    total_rss_kb += float(line.split()[1])
    except Exception:
        pass

    # 2. Read All Child Process VmRSS
    try:
        with open("/proc/self/task/children", "r") as f:
            children_pids = f.read().split()
            
        for cpid in children_pids:
            try:
                with open(f"/proc/{cpid}/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            total_rss_kb += float(line.split()[1])
            except (FileNotFoundError, ProcessLookupError):
                continue
    except Exception:
        pass

    return total_rss_kb / 1024.0  # Convert kB to MB


# Named top-level adapter for multiprocessing/ProcessPool (pickle requires module-level symbols)
def run_task_wrapper(_):
    return cpu_workload()

def benchmark_process_pool_executor():
    print(f"\n--- [2] Testing ProcessPoolExecutor ({WORKERS} Processes) ---")
    rss_before = get_process_rss_mb()
    t_start = time.perf_counter()
    
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        t_init = time.perf_counter() - t_start
        rss_after_init = get_process_rss_mb()
        
        t_exec_start = time.perf_counter()
        # Use top-level named function instead of lambda
        results = list(executor.map(run_task_wrapper, range(NUM_TASKS), chunksize=1))
        t_compute = time.perf_counter() - t_exec_start
        
    t_total = time.perf_counter() - t_start
    print(f"  * Init Time       : {t_init * 1000:.2f} ms")
    print(f"  * Compute Time    : {t_compute:.3f} s")
    print(f"  * Total Wall-Clock: {t_total:.3f} s")
    print(f"  * System Footprint: {WORKERS} full OS child processes + IPC pipes")
    return t_init, t_compute, t_total


def benchmark_raw_multiprocessing():
    print(f"\n--- [3] Testing multiprocessing.Pool ({WORKERS} Processes) ---")
    t_start = time.perf_counter()
    
    with mp.Pool(processes=WORKERS) as pool:
        t_init = time.perf_counter() - t_start
        
        t_exec_start = time.perf_counter()
        # Use top-level named function instead of lambda
        results = pool.map(run_task_wrapper, range(NUM_TASKS), chunksize=1)
        t_compute = time.perf_counter() - t_exec_start
        
    t_total = time.perf_counter() - t_start
    print(f"  * Init Time       : {t_init * 1000:.2f} ms")
    print(f"  * Compute Time    : {t_compute:.3f} s")
    print(f"  * Total Wall-Clock: {t_total:.3f} s")
    print(f"  * System Footprint: {WORKERS} OS forks/spawns + OS pipes")
    return t_init, t_compute, t_total        
    t_total = time.perf_counter() - t_start
    print(f"  * Init Time       : {t_init * 1000:.2f} ms")
    print(f"  * Compute Time    : {t_compute:.3f} s")
    print(f"  * Total Wall-Clock: {t_total:.3f} s")
    print(f"  * System Footprint: {WORKERS} OS forks/spawns + OS pipes")
    return t_init, t_compute, t_total


if __name__ == "__main__":
    # Ensure safe context for multiprocess on Linux
    mp.set_start_method("spawn", force=True)

    print(f"================================================================")
    print(f" BENCHMARK: {NUM_TASKS} Tasks | {WORKERS} Workers (Cores)")
    print(f"================================================================")

    res_mc = benchmark_mcloop()
    res_ppe = benchmark_process_pool_executor()
    res_mp = benchmark_raw_multiprocessing()

    print("\n================================================================")
    print("                      FINAL HEAD-TO-HEAD                        ")
    print("================================================================")
    print(f"{'Runtime':<24} | {'Init (ms)':<10} | {'Compute (s)':<12} | {'Total (s)':<10}")
    print("-" * 64)
    print(f"{'MCLoop (Subinterpreters)':<24} | {res_mc[0]*1000:<10.2f} | {res_mc[1]:<12.3f} | {res_mc[2]:<10.3f}")
    print(f"{'ProcessPoolExecutor':<24} | {res_ppe[0]*1000:<10.2f} | {res_ppe[1]:<12.3f} | {res_ppe[2]:<10.3f}")
    print(f"{'multiprocessing.Pool':<24} | {res_mp[0]*1000:<10.2f} | {res_mp[1]:<12.3f} | {res_mp[2]:<10.3f}")