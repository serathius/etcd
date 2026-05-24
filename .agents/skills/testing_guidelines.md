---
description: Test design patterns, production assertions vs test stubs, mock configurations, and copylocks avoidance.
globs: "**/*_test.go"
alwaysApply: false
---

# Go Testing & Verification Standards

These rules guide test layout, mocking/stubbing philosophies, config architecture, and static analysis safety across unit test files.

---

## 1. Testing and Stubbing Principles

### Production Quality over Defensive Test Stubs
Defensive `nil` checks in production runtime loops (e.g., `if r.storage != nil`) to accommodate minimal/lazy unit test initializations is an architectural anti-pattern.
- **DO** ensure the production code path operates with strict constraints (e.g., panicking or asserting on invalid dependencies at startup).
- **DO** refactor all unit test suites that pass stubbed/incomplete configs to supply proper mock stub implementations (e.g., `mockstorage.NewStorageRecorder("")`) instead of adding runtime guards.

### Fix Test Stubs Rather than Adding Production Nil Guards
- **DO NOT** "protect" the production path by adding a defensive nil guard check when a unit test triggers a panic because it passes a `nil` dependency (such as a `nil` config state in stubs).
- **DO** refactor the unit test setup to supply proper, mock, or stub initialized structures (e.g., `&raftpb.ConfState{}`), keeping the production code path highly cohesive, fast, and clean.

```go
// ❌ INCORRECT (Defensive production nil guard to satisfy minimal unit test stub)
func (r *Raft) Tick() {
    if r.storage != nil { // Architectural anti-pattern!
        r.storage.Tick()
    }
}

//  CORRECT (Assert production path, supply recorder/mock in test setup)
func (r *Raft) Tick() {
    r.storage.Tick() // Fast, unblocked execution path
}

// Test setup:
func TestRaftTick(t *testing.T) {
    r := &Raft{
        storage: mockstorage.NewStorageRecorder(""), // Pass proper initialized mock
    }
}
```

---


---

## 3. Test Design & Static Analysis (Copylocks)

### Avoid Struct Value Copies in Test Loops
When iterating over test case slices containing large struct configurations or types with internal locks (e.g., `raftpb.HardState` which contains internal mutexes), value-copy loops will trigger `copylocks` lints from `go vet`.
- **DO** always iterate by slice index rather than copying the struct value.
  ```go
  // Anti-Pattern:
  for _, tc := range tcs { ... } // Copies mutexes inside tc

  // Correct Pattern:
  for i := range tcs {
      tc := tcs[i] // or reference elements directly by index
  }
  ```

---

## 4. Standard Test Assertions & Comparisons

### Avoid Custom Assertion Functions in Tests
When comparing customized or pointer-allocated structures inside unit tests, avoid creating custom assertion or equality functions for each unique type comparison.
- **DO** convert both objects to a common standard type (e.g., using generic library constructors, pointer indirection, or marshalling) or use standard libraries.
- **DO** use standard, built-in deep comparison mechanisms (such as `reflect.DeepEqual` or standard `testify` assertion helper suites like `require.Equal`) directly inline within test files to keep validation blocks standardized, highly readable, and easily reviewable.

```go
// ❌ INCORRECT (Custom, ad-hoc assertion function created for a specific type comparison)
func assertMessagesEqual(t *testing.T, expected, actual []raftpb.Message) {
    if len(expected) != len(actual) {
        t.Errorf("length mismatch: expected %d, got %d", len(expected), len(actual))
    }
    for i := range expected {
        if expected[i].Type != actual[i].Type {
            t.Errorf("type mismatch at index %d", i)
        }
    }
}

//  CORRECT (Using inline assertions with standard deep-equality matching libraries)
require.Equal(t, expectedMessages, actualMessages)
```


