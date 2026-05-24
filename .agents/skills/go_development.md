---
description: Compiler version mismatches, gci import block structures, and Go 1.26 pointer allocations.
globs: ["**/*.go", "go.mod"]
alwaysApply: false
---

# Go Development & Tooling Standards

These rules govern compilation, toolchain version resolution, import ordering, style verification, and memory allocation patterns across Go codebases.

---

## 1. Toolchain and Compilation

### Compiler-Toolchain Version Mismatch
In environments with multiple toolchain versions, compilation commands (e.g., `go test`, `make verify`, `make build`) can fail.
- **DO** always run all build, test, and verification commands with the prefix `GOROOT=""` to ensure Go cleanly resolves to the active workspace toolchain.
  ```bash
  GOROOT="" make verify
  ```

---

## 2. Code Styling and Import Formatting (gci)

### Strict Import Block Structure
`golangci-lint` is configured with strict `gci` rules. Imports must be categorized into exactly three blocks separated by empty newlines:
1. **Standard Library:** (e.g., `testing`, `time`)
2. **Third-Party Default:** (e.g., `github.com/stretchr/testify/require`)
3. **Repository Prefix:** Packages beginning with the repository prefix `go.etcd.io` (e.g., `go.etcd.io/raft/v3/raftpb`).

- **DO NOT** group third-party imports and `go.etcd.io` imports into a single block. An empty newline must separate standard, default, and repository prefix groups.
- **DO NOT** leave duplicate empty newlines or trailing empty newlines at the end of files, as they will fail strict `gci` formatting verifications.

```go
// ❌ INCORRECT
import (
    "testing"
    "github.com/stretchr/testify/require"
    "go.etcd.io/raft/v3/raftpb"
)

//  CORRECT
import (
    "testing"
    "time"

    "github.com/stretchr/testify/require"

    "go.etcd.io/raft/v3/raftpb"
)
```

---

## 3. Go 1.26 Pointer Allocation Standards

### Direct Pointer Allocation (new)
Go 1.26 allows passing any complex expression, variable, or constant directly to the `new` built-in construct (e.g., `new(id)` or `new(uint64(1))`) to allocate heap memory and initialize values in a single expression.
- **DO NOT** create local temporary variable aliases (like `membID := uint64(memb.ID)`) simply to take their reference later (`&membID`). Use `new(expression)` inline directly inside struct declarations for cleaner, more memory-efficient code.
- **DO NOT** wrap a source variable inside a redundant conversion like `new(uint64(idx))` when the source variable is already declared as the target type (e.g., `idx uint64`). Use `new(idx)` directly to avoid `unconvert` compiler lints.
- **COMPILER COMPARISON NOTE**: Historically in pre-1.26 Go, `new(Type)` only accepted a Type name (e.g. `new(uint64)`), and passing a value constructor like `new(uint64(3))` triggered a compiler syntax error. In Go 1.26, `new(uint64(3))` is fully supported, idiomatic, and preferred over external library pointer helpers (like `proto.Uint64(3)`).

```go
// ❌ INCORRECT (Pre-Go 1.26 workarounds, redundant helper functions, or local temp alias variables)
id := uint64(1)
member := &Member{
    ID: &id,
}

// ❌ INCORRECT (Redundant conversion of variables already matching the target type)
idx := uint64(5)
val := new(uint64(idx))

//  CORRECT (Go 1.26 inline new allocation)
member := &Member{
    ID: new(uint64(1)),
}
```


