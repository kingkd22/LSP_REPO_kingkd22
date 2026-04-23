# Question 1 Answers

## Part 1:

**Shared Resource #1:** `nextId` - the integer counter used to generate unique request IDs

**Shared Resource #2:** `requests` - the ArrayList that stores all submitted request strings

**Concurrency Problem:** Race condition. Multiple threads can read and modify `nextId` and `requests` at the same time with no coordination, leading to duplicate IDs and corrupted list state.

**Why addRequest() is unsafe:** `addRequest()` performs two separate, non-atomic operations: calling `getNextId()` to read and increment `nextId`, then calling `requests.add()`. Between those two steps, another thread can come in and read the same value of `nextId` before it gets incremented, so two threads end up with the same ID. On top of that, `ArrayList` itself is not thread-safe, so concurrent calls to `requests.add()` can cause data loss or throw exceptions.

---

## Part 2:

**Fix A:** `public synchronized int getNextId() { ... }`
This fix is NOT correct. Synchronizing only `getNextId()` prevents two threads from generating the same ID at the same time, but it does not protect `addRequest()` as a whole. A thread can still be preempted after calling `getNextId()` and before calling `requests.add()`, and `ArrayList.add()` itself is still unsynchronized, so concurrent writes to the list can still corrupt it.

**Fix B:** `public synchronized void addRequest(String studentName) { ... }`
This fix IS correct. Synchronizing the entire `addRequest()` method means only one thread can execute the full sequence of getting an ID and adding the request at a time. Since `getNextId()` is only ever called from within `addRequest()`, the ID assignment and the list insertion are now one atomic unit from the perspective of any caller.

**Fix C:** `public synchronized List<String> getRequests() { ... }`
This fix is NOT correct. Synchronizing the getter only protects reads of the list reference but does nothing to protect `addRequest()` while it is modifying `nextId` and `requests`. The race conditions in the write path are completely unchanged.

---

## Part 3:

**Answer:** No, `getNextId()` should not be public.

**Explanation:** According to Riel's heuristics, a class should hide its implementation details and expose only what external clients need. `getNextId()` exists solely to support the internal logic of `addRequest()`. No outside caller should be calling it directly, because doing so bypasses whatever synchronization or sequencing the class needs to maintain correctness. Making it public breaks encapsulation and invites misuse. It should be private so that the class controls exactly when and how IDs are assigned.

---

## Part 4:

**Description:**
The alternative approach discussed in lecture is using classes from `java.util.concurrent` instead of the `synchronized` keyword. Specifically, `AtomicInteger` provides atomic compare-and-set operations on integers without needing explicit locks, and `CopyOnWriteArrayList` is a thread-safe list implementation. Using these two together makes `addRequest()` thread-safe because the ID increment is a single atomic operation and all list writes are internally synchronized.

**Code Snippet:**
```java
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

private AtomicInteger nextId = new AtomicInteger(1);
private List<String> requests = new CopyOnWriteArrayList<>();

public void addRequest(String studentName) {
    int id = nextId.getAndIncrement();
    String request = "Request-" + id + " from " + studentName;
    requests.add(request);
}
```
