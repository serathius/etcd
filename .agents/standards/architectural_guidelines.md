---
description: Directory tree macro layout, Step-down ordering rule, and comments policy.
globs: "**/*.go"
alwaysApply: false
---

# Architectural Guidelines

These standards govern directory structures, code layout within files, and comments/documentation inside Go files to ensure high cohesion, clear narrative flow, and clean reviewable structures.

---

## 1. Directory Architecture (Macro)

### DO
- **Do** ensure the directory tree mirrors the logical hierarchy of the application domain, not the framework.
- **Do** ensure every directory solves a single, well-defined problem (Bounded Context).
- **Do** mirror the dependency graph in the file tree, adhering to "Vertical Slice" principles.
- **Do** treat a module `B` as a "Private Implementation Detail" of `A` and move it into a subdirectory of `A` if `A` is the exclusive dependent of `B`.
- **Do** prioritize keeping code that changes together in the same directory (Locality of Behavior).

### DON'T
- **Do not** separate files by "File Type" (e.g., placing all styles in one folder, all controllers in another) if it forces developers to jump across the tree.
- **Do not** create generic "drawer" directories (e.g., `utils`, `common`, `helpers`) unless the code is truly domain-agnostic generic logic.

---

## 2. Internal File Organization (Micro)

### DO
- **Do** break large files into smaller units. If a specific set of functions constitutes a separate logical unit, move them to their own file.
- **Do** organize code according to the **Step-down Rule**:
  1. **Public API:** High-level entry points and exports at the top.
  2. **Narrative Flow:** Define functions close to where they are used (if function `A` calls `B`, place `B` directly below `A`).
  3. **Details Last:** Low-level helpers and utilities at the bottom of the file.

---

## 3. Comments & Documentation

### DO
- **Do** write comments *only* to explain **why** a certain path or design was taken, such as documenting benchmark results to explain a constant selection.
- **Do** write self-documenting code using explicit naming conventions and robust structure.

### DON'T
- **Do not** explain *what* the code is doing in comments. If the code requires explanations of its actions, refactor the structure, rename variables, or extract subfunctions instead.

