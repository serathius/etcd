---
description: Robustness verification, failpoint injection via gofail, and historical regression reproduction commands.
globs: ["**/*_test.go", "Makefile", "**/gofail*"]
alwaysApply: false
---

# Robustness & Failpoint Testing Guidelines

Use these standards to execute robustness testing workflows, configure mock/failpoint injections using `gofail`, and run commands to reproduce historical regressions in the etcd codebase.

---

## 1. Robustness Testing Overview

Robustness tests evaluate the correctness of etcd under extreme conditions (network partitions, slow disks, process crashes, etc.) using injection techniques.

- **DO** run basic robustness testing via:
  ```bash
  GOROOT="" make test-robustness
  ```

---

## 2. Failpoint Injection (gofail)

Failpoint injection is managed using `gofail`. Failpoints must be enabled before compiling and running tests, and cleanly disabled/tidied up afterward.

- **DO** enable failpoints before running robustness tests:
  ```bash
  GOROOT="" make gofail-enable
  ```
  This runs `gofail enable` on key sub-packages (e.g., `server/etcdserver/`, `server/storage/mvcc/`, `server/storage/wal/`) and updates module configurations.

- **DO** disable failpoints and clean up Go modules via `go mod tidy` immediately after completing tests:
  ```bash
  GOROOT="" make gofail-disable
  ```

---

## 3. Historical Issue Reproduction

To reproduce regressions or validate that a fix solves a known robustness issue, **DO** run the corresponding regression tests with high count and failfast:

### Issue 14370 (Regression Test)
```bash
GOROOT="" GO_TEST_FLAGS='-v --run=TestRobustnessRegression/Issue14370 --count 100 --failfast --bin-dir=/tmp/etcd-v3.5.4-failpoints/bin' make test-robustness
```

### Issue 17780 (Compaction Race)
```bash
GOROOT="" GO_TEST_FLAGS='-v --run=TestRobustnessRegression/Issue17780 --count 200 --failfast --bin-dir=/tmp/etcd-v3.5.13-compactBeforeSetFinishedCompact/bin' make test-robustness
```

### Issue 19179
```bash
GOROOT="" GO_TEST_FLAGS='-v -run=TestRobustnessRegression/Issue19179 -count 200 -failfast --bin-dir=/tmp/etcd-v3.5.17-failpoints/bin' make test-robustness
```
