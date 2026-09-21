from concurrent import interpreters
import sys
from queue import Queue
import time


# shut down signal for the LOOP. 
_SENTINEL = "__MCLOOP_SHUTDOWN__"

# sentinel worker loop. 
def _worker_loop(tasks_queue, results_queue):
    while True:
        worker_task = tasks_queue.get()

        # break if called for shut down. 
        if worker_task == "__MCLOOP_SHUTDOWN__":
            break

        # execute the user job. 
        try:
            job_id, fn, args, memory_cost = worker_task
            results = fn(*args)
            results_queue.put((job_id, results, None, memory_cost))

        except Exception as e:
            results_queue.put((job_id, None, str(e), memory_cost))


class MCLoop:
    """
    Create a pool of interpreters. 
    """
    def __init__(self, no_of_interpreters: int = 2, total_memory: int = 100 * 1024 * 1024):
        self.total_interpreters: int = no_of_interpreters
        self.total_memory: int = total_memory
        self.current_in_flight_memory: int = 0

        # create the task and results queue. 
        self.tasks_queue = interpreters.create_queue(maxsize=128)
        self.results_queue = interpreters.create_queue()

        # create the metrics holders.
        self.interpreters: list = []
        self.pending_jobs: dict = {}
        self._job_counter: int = 0 

        # store the sub interpreter worker handles, to close them gracefully 
        # when shutting down. 
        self._worker_handles: list = []

        # create the interpreters and launch them for accepting workloads. 
        self._bootstrap_workers()

    def _bootstrap_workers(self) -> None:
        """
        Create the sub interpreters and launch them for workload executions. 
        """

        for _ in range(self.total_interpreters):
            new_interpreter = interpreters.create()
            self.interpreters.append(new_interpreter)

            # spin up a dedicated OS background for each interpreter. 
            # bind the worker loop here, so it keeps executing infinitely for new coming jobs. 
            ip_thread_handle = new_interpreter.call_in_thread(_worker_loop, self.tasks_queue, self.results_queue)

            # add the interpreter thread handle to the list. 
            self._worker_handles.append(ip_thread_handle)


    # estimate the size of the payload. Too big payloads will stall the pool.
    # this is done to prevent OOM errors from the provided memory size during initialization. 
    def _estimate_payload_size(self, *args) -> int:
        """
        estimate the payload size of each job. 
        """
        payload_size = 0

        for argument in args:
            if isinstance(argument, (bytes, bytearray, memoryview)):
                payload_size += len(argument)
            else:
                payload_size += sys.getsizeof(argument)

        return payload_size


    def poll(self):
        """
        update the status of jobs in the pending jobs collection. 
        """
        while not self.results_queue.empty():
            job_id, job_result, job_error, job_memory_cost = self.results_queue.get_nowait()

            # calculate the current available inflight memory. 
            self.current_in_flight_memory = max(0, self.current_in_flight_memory - job_memory_cost)

            # update the status of the job id inthe pending jobs dictionary (fast)
            if job_id in self.pending_jobs:
                self.pending_jobs[job_id]["result"] = job_result
                self.pending_jobs[job_id]["error"] = job_error
                self.pending_jobs[job_id]["ready"] = True


    def submit(self, fn, *args) -> int:
        """
        Submit the workload to the queue. 
        """
        # find the memory cost of the current payload size. 
        memory_cost = self._estimate_payload_size(*args)

        # check memory, if full, poll(). 
        # if still not cleared, then raise BufferError()
        if self.current_in_flight_memory + memory_cost > self.total_memory:
            self.poll()
            if self.current_in_flight_memory + memory_cost > self.total_memory:
                raise BufferError("Insufficient memory")

        self._job_counter += 1
        job_id = self._job_counter

        # set up the job in the pending jobs as a dictionary. 
        self.pending_jobs[job_id] = {"ready": False, "result": None, "error": None} 

        # increment the memory cost now, after job is added. 
        self.current_in_flight_memory += memory_cost

        # put the job in the task queue. 
        self.tasks_queue.put((job_id, fn, args, memory_cost))

        return job_id


    def fetch(self, job_id, timeout: float = None):
        start_time = time.monotonic()

        while True:
            self.poll()
            if self.pending_jobs.get(job_id, {}).get("ready"):
                entry = self.pending_jobs.pop(job_id)
                if entry["error"]:
                    raise RuntimeError(f"Job {job_id} failed: Error {entry['error']}")

                return entry["result"]

            # check the time boundary here. 
            if timeout is not None and (time.monotonic() - start_time) > timeout:
                raise TimeoutError(f"Job {job_id} timeout!")

            # let the event loop circle back withing suspended tasks
            # to check for completion. 
            time.sleep(0.001)



    def close(self):
        """
        Shutdown all the sub interpreters. 
        """
        # put one SENTINEL per sub interpreter. 
        for _ in range(self.total_interpreters):
            self.tasks_queue.put(_SENTINEL)

        # tear down the sub interpreter threads. 
        for sip_thread_handle in self._worker_handles:
            sip_thread_handle.join()
        
        # close the sub interpreters. 
        for sub_interpreter in self.interpreters:
            sub_interpreter.close()


        # clear the sub interpreters list. 
        self.interpreters.clear()





