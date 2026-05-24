---
description: Make validation speedups, lint check optimization, and concurrent race stress testing CLI commands.
globs: ["Makefile", "**/*.go"]
alwaysApply: false
---

# Verification Workflows & CLI Speeds

These guidelines detail speed optimizations for local verification checks and concurrent stress testing methodologies.

---

## 1. Validation Workflow Speed Optimizations

### Speed vs Accuracy Tradeoff
The full `make verify` suite executes over 14 distinct sub-stages, many of which involve heavy script or document verifications. This is extremely expensive to run iteratively.

- **DO** optimize iterative loops by running only targeted Go static checks for normal code changes:
  * **Iterative styling/import/lint verification:** Use `GOROOT="" make verify-lint` instead of `make verify` to get sub-second lint and styling feedback.
  * **Iterative test verification:** Run only targeted unit tests `GOROOT="" go test ./path/to/package -run TestName` instead of the full unit suite.
- **DO NOT** run global aggregate fix commands (like `make fix`) when a specific subrule check fails (such as `verify-lint` or `verify-mod-tidy`). Immediately run the corresponding targeted subrule fix (e.g., `GOROOT="" make fix-lint` or `GOROOT="" make fix-mod-tidy`) to avoid massive repository-wide overhead and prevent auto-formatting files outside your scope.
- **DO NOT** modify files outside the requested Bounded Context (e.g., resolving linter failures in metadata or auxiliary directories like `.gemini/` or unrelated sub-packages) unless explicitly directed by the user. Focus strictly on the files and packages under active request.
- **DO** run the full `GOROOT="" make verify && GOROOT="" make build && GOROOT="" make test-unit` suite *only* as a final sanity check before final pushes or peer-review deliverables.

---

## 2. Safe Deep-Equality Comparisons for Protobuf Message Structs

When refactoring unit test assertions using `github.com/google/go-cmp/cmp` on generated protocol buffer message structures:

- **DO NOT** attempt a naive struct slice or direct struct comparison (`cmp.Diff(ents, entries)`). Standard struct comparison will crash or panic when encountering unexported protobuf runtime internal metadata properties (e.g. `state`, `sizeCache`, `unknownFields`).
- **DO** explicitly ignore unexported fields by passing **`cmpopts.IgnoreUnexported(raftpb.Entry{})`** (specifying the target message struct type) or **`protocmp.Transform()`** combined with explicit message-field ignores in the `cmp.Diff` option block. This guarantees stable, deterministic structural diff evaluations without runtime panics.
- **DO NOT** use `cmp.Diff` or `cmp.Equal` directly on high-volume data arrays or maps (e.g. 10MB–40MB WAL payloads containing thousands of entries). Constructing deep-equality recursive diff trees on huge maps triggers combinatorial explosion and severe garbage collection memory barrier overheads, resulting in test hangs and timeouts.
- **DO** optimize comparisons on large datasets by manually comparing length, stripping/setting heavy payload properties (like `Data = nil`) before running struct-field diffs, or evaluating raw payload slices directly inside a fast loop using optimized `bytes.Equal` calls.

```go
// ❌ INCORRECT (Naive protobuf deep-equality comparison that crashes or panics)
if diff := cmp.Diff(expectedEntries, actualEntries); diff != "" {
    t.Errorf("unexpected diff:\n%s", diff)
}

//  CORRECT (Explicitly ignoring unexported protobuf fields to prevent crashes)
if diff := cmp.Diff(expectedEntries, actualEntries, cmpopts.IgnoreUnexported(raftpb.Entry{})); diff != "" {
    t.Errorf("unexpected diff:\n%s", diff)
}

//  CORRECT (Alternative using protocmp transformer for complex nested structures)
if diff := cmp.Diff(expectedMsg, actualMsg, protocmp.Transform()); diff != "" {
    t.Errorf("unexpected diff:\n%s", diff)
}
```

