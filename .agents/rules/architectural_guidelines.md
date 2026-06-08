---
description: Directory tree macro layout, Step-down ordering rule, comments policy, and AI-agent behaviors for Go.
globs: "**/*.go"
alwaysApply: true
---

# Architectural Guidelines

These standards govern directory structures, code layout within files, documentation, and AI-agent interactions inside Go files. They ensure high cohesion, clear narrative flow, clean reviewable structures, and predictable multi-agent collaboration.

---

## 1. Directory Architecture (Macro)

### DO
- **Mirror the Domain:** Ensure the directory tree mirrors the logical hierarchy of the application domain, not the web framework.
- **Bounded Contexts:** Ensure every directory solves a single, well-defined problem. This strict isolation is required so parallel AI agents can work concurrently without causing merge conflicts or shared-state corruption.
- **Locality of Behavior:** Prioritize keeping code that changes together in the same directory.
- **Private Implementation:** Treat a module `B` as a "Private Implementation Detail" of `A` and move it into a subdirectory of `A` if `A` is the exclusive dependent.

### AVOID (AI Constraints)
- **Avoid** generic "drawer" directories (e.g., `utils`, `common`, `helpers`). Instead, group generic logic by its specific technical domain (e.g., `stringsvc`, `mathutil`).
- **Avoid** separating files purely by "File Type" (e.g., placing all controllers in one folder and all models in another). 

---

## 2. Internal File Organization (Micro)

### DO
- **Granular Modularity:** Break large files into smaller units. If a specific set of functions constitutes a separate logical unit, extract them to a new file.
- **The Step-down Rule:** Organize code sequentially within the file:
  1. **Public API:** High-level entry points and exported structs/interfaces at the top.
  2. **Narrative Flow:** Define functions close to where they are used (if function `A` calls `B`, place `B` directly below `A`).
  3. **Details Last:** Low-level helpers and unexported utilities at the bottom.

---

## 3. Comments & Documentation

### DO
- **Document Intent:** Write comments *only* to explain **why** a certain path, constraint, or design was taken (e.g., documenting benchmark results to explain a constant selection).
- **Self-Documenting Code:** Rely on explicit naming conventions and robust code structure over inline comments.
- **Standard GoDocs:** Write standard `// SymbolName ...` comments for exported APIs to assist IDE language servers and downstream documentation agents.

### AVOID
- **Avoid** explaining *what* the code is doing. If an agent or human requires an explanation of the actions, refactor the structure, rename variables, or extract subfunctions instead.

---

## 4. AI Agent & Antigravity Workflow Rules

### DO
- **Fix Causes, Not Symptoms:** When debugging an error, test failure, or unexpected output, halt and analyze the root cause. Attempt a maximum of one fix at a time. Never apply a workaround to bypass an underlying architectural flaw.
- **Generate Implementation Plans:** Before modifying the codebase, output a brief technical plan outlining which Bounded Contexts will be touched to prevent scope creep.
- **Parallel-Safe Testing:** Ensure all generated Go tests do not rely on shared global state or hardcoded file paths so that multiple agents can execute tests in parallel.