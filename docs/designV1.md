Here is the revised, memory-optimized plan.

The primary change shifts the architecture away from per-job thread spawning (`call_in_thread()` per task) or paired per-interpreter channels, which cause allocation churn and unbounded buffer growth. Instead, it adopts **long-lived persistent worker loops** consuming from a **bounded, shared queue**, with strict memory tracking using zero-copy / buffer-aware serialization.

---

### Step 1: Manage Interpreter State & Pre-warmed Worker Daemons

* Create the target number of interpreters once during `__init__` via `interpreters.create()`.
* **Zero Dispatch Allocation:** Launch a persistent worker loop inside each interpreter *once* at initialization using `interp.call_in_thread(worker_loop, in_q, out_q)`.
* The worker thread remains alive for the lifetime of `MCLoop`, eliminating the repeated memory allocations and thread-creation overhead of ad-hoc dispatches.
* Interpreters no longer need explicit `IDLE`/`BUSY` tracking in Python state; the work-stealing queue naturally balances tasks across both workers.

### Step 2: Bounded, Low-Footprint Cross-Interpreter Queues

* Instead of arbitrary paired channels, instantiate bounded queues:
* `task_queue = interpreters.create_queue(maxsize=N)`: Prevents memory blowup if the main loop produces tasks faster than the two interpreters can consume them.
* `result_queue = interpreters.create_queue()`: Holds completed task payloads.


* Restrict passing heavy nested Python dictionaries or uncompiled strings. Pass **compact primitive tuples**:

$$\text{Task Envelope} = (\text{job\_id: int}, \text{fn: callable}, \text{args: tuple})$$


* For large payloads (arrays, images, tabular data), pass **read-only `memoryview` / `bytes` buffers** rather than standard Python lists, enabling near zero-copy reads across the interpreter boundary.

### Step 3: Streamlined Dispatch with Logical Memory Throttling

* Implement `submit(job_fn, *args)`:
1. **Memory Budget Check:** Check estimated payload size against `total_memory`. If `current_in_flight_memory + task_size > total_memory`, either backpressure (block/yield) or raise an exhaustion error.
2. Increment the `current_in_flight_memory` counter.
3. Push the task envelope into `task_queue`.


* When passing tasks, prefer referencing top-level functions already present or imported inside the worker environment to avoid serializing entire function code blocks.

### Step 4: Non-Blocking Event Loop & Garbage Reclaiming

* In `poll()` or `run_until_complete()`:
* Check `result_queue` non-blockingly (`get_nowait()`).
* On arrival of a result envelope `(job_id, result_data, memory_weight)`:
1. Resolve the pending task record.
2. Immediately decrement `current_in_flight_memory` by `memory_weight`.
3. Explicitly delete references (`del result_data`) inside the consumer loop to promptly trigger reference deallocation rather than waiting for cyclical GC passes.




* Provide an asynchronous wait mechanism (using small sleeps or OS notification pipes) so the event loop doesn't spin at 100% CPU when waiting on result channels.

### Step 5: Clean Shutdown Protocol

* Implement `close()` / `shutdown()`:
* Push sentinel tokens (e.g., a special `_SHUTDOWN` singleton) equal to the number of interpreters into `task_queue`.
* Each worker receives the sentinel, breaks its internal `while True` loop, cleans up its local references, and exits.
* Destroy interpreter handles via `interpreters.destroy(interp)` to return all interpreter-specific heap segments back to the process allocator.



---

### Revised Reality Check: `total_memory` Enforcement

Because subinterpreters share the process heap, memory control is **governed through ingress throttling**:

* `total_memory` functions as an **in-flight memory semaphore**.
* By pairing a bounded queue (`maxsize`) with payload-size tracking before enqueueing, you effectively cap the maximum un-garbage-collected memory circulating across both interpreters at any given millisecond.