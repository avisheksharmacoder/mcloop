from core import MCLoop

def job(n: int) -> int:
    total = 0
    for i in range(n):
        total += 1
    return total

custom_loop = MCLoop(2, 200_000_000)
job_id = custom_loop.submit(job, (5000,))
print(custom_loop.fetch(job_id=job_id))
