---
description: Benchmarking isolation, system idle controls, perflock CPU limiting, and benchstat comparative statistical analysis.
globs: ["**/*_test.go", "Makefile", "bench_results.txt"]
alwaysApply: false
---

# Go Benchmarking Standards

These standards govern performance evaluation, execution environment isolation, thermal throttling mitigation, and statistical analysis to ensure reproducible and comparable results.

---

## 1. Benchmarking Invariants

### System Setup & Idle State
- **DO** if you see high variance in results, ask user to close resource-hungry background applications (such as Slack, browsers, or media players) before beginning benchmarks to keep baseline CPU utilization below 2-3%. **Don't** close anything youself.
- **DO** control CPU thermal throttling. Modern CPUs (especially laptops) throttle speed under sustained load, distorting long runs. On Linux, **DO** run benchmarks using `perflock` to lock the CPU to a constant, sustainable speed limit (e.g., 70%):
  ```bash
  # Run benchmark locked to a stable CPU limit
  perflock go test -bench=. -count=6 | tee bench_results.txt
  ```

### Isolation
- **DO** use `-run=^$` to ensure only benchmarks run, avoiding interference from regular unit tests.
- **DO** run benchmarks multiple times (prefer `-count=6` to `-count=10` depending on base variance) to ensure statistical significance.

### Output Management
- **DO** always pipe output to `tee` to view progress while saving to a file for analysis.
  ```bash
  GOROOT="" go test -bench=. -count=6 | tee bench_results.txt
  ```

---

## 2. Comparative Analysis (benchstat)

- **DO** use `benchstat` to compare results instead of manual or direct percentage checks. `benchstat` uses P-value statistical tests to identify if a performance delta is actually significant or just background noise.
- **DO** name sub-benchmarks using `Key=Value` format (e.g., `BenchmarkFoo/Interning=True`) to allow `benchstat` to pivot results.
  ```go
  // ❌ INCORRECT (Generic or unstructured sub-benchmark names)
  b.Run("without_interning", func(b *testing.B) {
      // ...
  })

  //  CORRECT (Key=Value structured formatting for benchstat compatibility)
  b.Run("Interning=False", func(b *testing.B) {
      // ...
  })
  ```
- **DO** use `benchstat` with the `-col` flag to compare scenarios across a specific dimension:
  ```bash
  benchstat -col=/Interning bench_results.txt
  ```
- **DO establish baseline and compare impact:**
  1. **Establish Baseline:** Run the benchmarks with a set configuration (e.g., `-count=6` or `-count=10` with `perflock`) on the unmodified branch to record baseline performance.
  2. **Establish Change Impact:** Run the identical benchmark suite on the modified branch.
  3. **Compare and Reject/Accept:** Use `benchstat` to compare the baseline results file against the new results file. **Reject your hypothesis** for the performance improvement if `benchstat` reports no statistically significant difference (e.g., overlapping confidence intervals, high p-value, or no difference) or a performance degradation.
  4. **Document the Difference:** When presenting the benchmark results, always show the raw comparative `benchstat` output to verify that the change indeed made a measurable and statistically significant difference.
