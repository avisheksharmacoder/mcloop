from concurrent import interpreters
import sys
import time

import asyncio
import socket
import os


# for test. 
# Inside src/core.py
def compute_hash(n: int) -> int:
    val = 0
    for i in range(n):
        val = (val ^ (i * 2654435761)) & 0xFFFFFFFF
    return val


# shut down signal for the LOOP. 
_SENTINEL = "__MCLOOP_SHUTDOWN__"

# sentinel worker loop. 
def _worker_loop(tasks_queue, results_queue, notify_fd: int):
    """
    This worker loop runs isolated inside a subinterpreter. 
    Executes tasks and signals the main reactor via the OS kernel descriptor. 
    """
    while True:
        try:
            worker_task = tasks_queue.get()
        except Exception as e:
            # Prevent the silent worker death, if queue get or unpickling raises. 
            results_queue.put((-1, None, f"Error received: {e}", 0))

            try:
                os.write(notify_fd, b"\x01")
            except (BlockingIOError, OSError):
                pass
            continue

        # break if called for shut down. 
        if worker_task == _SENTINEL:
            break

        # get the job id, function, arguments, memory cost from the task. 
        job_id, fn, args, memory_cost = worker_task

        # execute the user job. 
        try:
            results = fn(*args)
            results_queue.put((job_id, results, None, memory_cost))

        except Exception as e:
            results_queue.put((job_id, None, str(e), memory_cost))

        finally:
            # wake up the main interpreter event loop via a non blocking os write.
            try:
                os.write(notify_fd, b"\x01")
            except (BlockingIOError, OSError):
                # If the buffer is full, the main loop is already scheduled to wake up. 
                pass 


class MCLoop:
    """
    Create a pool of interpreters.
    The pool is driven by an OS descriptor Reactor bridge. 
    """
    def __init__(
            self, 
            no_of_interpreters: int = 2, 
            total_memory: int = 100 * 1024 * 1024,
            loop: asyncio.AbstractEventLoop | None = None
    ):
        self.total_interpreters: int = no_of_interpreters
        self.total_memory: int = total_memory
        self.current_in_flight_memory: int = 0
        self.loop = loop or asyncio.get_event_loop()

        # create the tasks and results MPMC queues. 
        self.tasks_queue = interpreters.create_queue()
        self.results_queue = interpreters.create_queue()

        # OS notification channels using Python stdlib sockets. 
        self._read_sock, self._write_sock = socket.socketpair()
        self._read_sock.setblocking(False)
        self._write_sock.setblocking(False)
        self.notify_fd = self._write_sock.fileno()

        # job tracking and futures. 
        self.pending_futures: dict[int, asyncio.Future] = {}
        self._job_counter: int = 0 

        # create the metrics holders.
        self.interpreters: list = []

        # store the sub interpreter worker handles, to close them gracefully 
        # when shutting down. 
        self._worker_handles: list = []

        # hook the read socket into the asyncio reactor. 
        self.loop.add_reader(self._read_sock.fileno(), self._on_worker_notify)

        # create the interpreters and launch them for accepting workloads. 
        self._bootstrap_workers()


    def _bootstrap_workers(self) -> None:
        """
        Create the sub interpreters and launch them for workload executions. 
        Also sync the environment paths for the new subinterpreters. 
        """
        # sync parent paths for the worker loops. 
        sync_path_code = f"import sys\nsys.path[:] = {sys.path!r}\n"

        for _ in range(self.total_interpreters):
            new_interpreter = interpreters.create()
            self.interpreters.append(new_interpreter)

            new_interpreter.exec(sync_path_code)

            # spin up a dedicated OS background for each interpreter. 
            # bind the worker loop here, so it keeps executing infinitely for new coming jobs. 
            ip_thread_handle = new_interpreter.call_in_thread(
                _worker_loop, 
                self.tasks_queue, 
                self.results_queue,
                self.notify_fd
            )

            # add the interpreter thread handle to the list. 
            self._worker_handles.append(ip_thread_handle)


    def _on_worker_notify(self) -> None:
        """
        Reactor callback fired by the OS when any subinterpreter writes a byte.
        Drains both the notification and the results queue. 
        """

        # Drain the notification bytes, so the descriptor resets in the kernel. 
        try:
            while True:
                chunk = self._read_sock.recv(4096)
                if not chunk:
                    break

        except (BlockingIOError, InterruptedError):
            pass

        # Drain all the completed results in the queue, 
        # if it is found to be filled up. 
        while not self.results_queue.empty():
            try:
                job_id, result, error, memory_cost = self.results_queue.get_nowait()

            except Exception:
                break

            # reduce the inflight memory size by the workload size. 
            self.current_in_flight_memory = max(0, self.current_in_flight_memory - memory_cost)

            future = self.pending_futures.pop(job_id, None)
            if future and not future.done():
                if error is not None:
                    future.set_exception(RuntimeError(error))
                else:
                    future.set_result(result)



    # estimate the size of the payload. Too big payloads will stall the pool.
    # this is done to prevent OOM errors from the provided memory size during initialization. 
    def _estimate_payload_size(self, *args) -> int:
        """
        Estimate the payload size of each job for memory tracking (approx) 
        """
        payload_size = 0

        for argument in args:
            if isinstance(argument, (bytes, bytearray, memoryview)):
                payload_size += len(argument)
            else:
                payload_size += sys.getsizeof(argument)

        return payload_size



    def submit(self, fn, *args) -> int:
        """
        Submit the workload to the queue. 
        """
        # find the memory cost of the current payload size. 
        memory_cost = self._estimate_payload_size(*args)

        # check memory, if full, poll(). 
        # if still not cleared, then raise BufferError()
        if self.current_in_flight_memory + memory_cost > self.total_memory:
            raise BufferError("Insufficient memory")

        self._job_counter += 1
        job_id = self._job_counter

        # set up the job in the pending jobs as a dictionary. 
        future = self.loop.create_future()
        self.pending_futures[job_id] = future

        # increment the memory cost now, after job is added. 
        self.current_in_flight_memory += memory_cost

        # put the job in the task queue. 
        self.tasks_queue.put((job_id, fn, args, memory_cost))

        return future


    def close(self):
        """
        Tear down descriptors and close subinterpreters. 
        """

        # first, unregister from the event loop. 
        try:
            self.loop.remove_reader(self._read_sock.fileno())
        except Exception:
            pass

        # put one SENTINEL per sub interpreter to close it down. 
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
        self._worker_handles.clear()

        # Close the OS sockets. 
        self._read_sock.close()
        self._write_sock.close()





