---
description: Deflaking mindsets, reproduction workflows (stress/race/delay-injection), deadlock debugging, and common test-suite timing anti-patterns.
globs: "**/*_test.go"
alwaysApply: false
---

# Test Deflaking & Stress Testing Standards

These guidelines outline the core mindsets, debugging strategies, reproduction methods, and anti-patterns for investigating, reproducing, and resolving flaking tests in the etcd codebase.

---

## 1. The Deflaking Mindset

Every flaking test signals an issue in one of three places:
1. **The system under test:** An underlying race condition, consensus regression, deadlock, or leak in the runtime (e.g., raft state machine, MVCC backend, wal, transport).
2. **The test itself:** Brittle assumptions, poor mocking, shared state, or lack of synchronization.
3. **The execution environment (CI):** Resource constraints, CPU starvation, or disk congestion.

### Core Invariants
- **DO NOT** assume a flake is always infrastructure or "test-only." Investigate and verify the exact root cause of a failure before writing a patch.
- **DO** verify that a component is designed to operate asynchronously before introducing any polling or delays.
- **DO** isolate the source of latency before adjusting any timeout. Fix the performance bug if latency is unexpected.
- **DO NOT** alter tests to bypass a concurrency failure if doing so weakens the validity of the assertion. Never implement "fake fixes."

---

## 2. Establishing a Reproducible Baseline

- **DO** establish a solid, local reproduction baseline before attempting to write a fix for a flaking test.

### Delay & Timing Lost Races
- **Delay Injection:** Temporarily inject intentional latency (e.g., `time.Sleep(time.Second)`) at critical asynchronous boundaries in the **production code** (e.g., at the beginning/end of a goroutine, watch event handler, or worker sync loop) to force timing windows to open up.
- **Timing Lost Races:** Force the test client to lose asynchronous races by adding temporary delays (e.g., `time.Sleep(100 * time.Millisecond)`) between test steps.

### Standard Stress Workflows
- **Bypass Go Cache:** Always run with `-count=1` to bypass Go test caches when searching for flakes:
  ```bash
  GOROOT="" go test ./server/etcdserver -count=1 -run=TestName
  ```
- **Stress Compilation and Stress Testing:**
  1. Compile the target package test binary with the race detector enabled:
     ```bash
     GOROOT="" go test -race -c ./server/etcdserver
     # Or specifically target standard packages:
     GOROOT="" go test -c -race ./path/to/package
     ```
  2. Run the compiled test binary with high concurrency under the `stress` tool, specifying parallel workers and timeouts:
     ```bash
     stress -p 8 ./etcdserver.test -test.run=TestName -test.timeout=10s
     ```
- **Verify mathematically:**
  1. **Establish Baseline:** Before applying any fix, run the flaky test under stress (e.g., using the `stress` tool or a high count run `go test -count=100`) to calculate the baseline error rate (failures / total runs).
  2. **Establish the Change Impact:** After applying the proposed fix, rerun the identical stress command/workflow with the same number of iterations.
  3. **Compare and Reject/Accept:** Compare the pre-change error rate against the post-change error rate. **Reject your hypothesis** for the fix if the change does not show a distinct, reproducible reduction in the flakiness or error rate compared to the baseline.
  4. **Document the Difference:** When presenting the solution, always provide the side-by-side comparison of the pre-fix stress/run results versus the post-fix results to prove the difference was actually made.


---

## 3. Debugging Deadlocks & Timeout Failures

If a package or suite is deadlocking or hitting timeouts under stress:
1. **DO** profile and run individual tests to determine their average successful runtime.
2. **DO** stress-test the individual test with a tight timeout limit set to ~100x its average successful runtime. This fails the execution quickly instead of hanging for minutes.
3. **DO** isolate and instrument the test with temporary structured debug logs around select channels, lock acquisitions, and goroutine exits to pinpoint the blockpoint.

---

## 4. Avoid Common Test Anti-Patterns

- **Non-Deterministic Iteration Assertions:** Go's map iteration is randomized by design. Never assert exact ordering or exact slice content matches on elements retrieved directly from standard Go map iterations. Sort keys or values prior to assertion.
- **Tight Timing Tolerances:** Avoid strict, low timeout limits (such as `100ms` or `500ms`) for functional checks. CI environments run under heavy parallel loads and resource-constrained nodes. Use generic, highly tolerant boundaries (e.g., `wait.ForeverTestTimeout` or robust watch event triggers) unless writing a specific micro-benchmark or performance test.
- **Fake/Mock Watch Relisting:** Mock or fake client watchers can relist/rewatch at arbitrary times due to network, compaction, or library behaviors. Assert on specific expected actions or key events rather than expecting a static sequence of mock calls.
- **Resource Conflicts:** Never hardcode static ports (e.g., `:2379`) or fixed temporary directories in unit/integration tests. Use `:0` for automatic TCP port allocation, and dynamically allocated unique test directories to prevent collision during concurrent execution.

---

## 5. Log Collection and Correlation

For complex integration or E2E cluster failures:
1. Gather logs from all participating nodes (all etcd member peers, client instances, proxies).
2. Filter logs closely around the timestamp of the failure.
3. Prefix log lines with their respective node/component identifier, sort the combined output chronologically, and analyze the sequence of events leading up to the error.
