---
description: Session tracking roadmaps, staged checklists, complexity prioritization, and comment resync strategies.
globs: ["**/*.json", "**/*.md"]
alwaysApply: false
---

# PR Roadmap & Session Tracking Workflows

Use these standards to create, update, and iteratively execute local session tracking artifacts during review iterations.

---

## 1. Staged Complexity Roadmaps

When handling a PR with a substantial number of comments, structure a roadmap to optimize engineering momentum:

- **DO** save retrieved comments into raw local JSON files (e.g., `comments_raw.json`) in your Gemini brain directory (`/usr/local/google/home/siarkowicz/.gemini/jetski/brain/<conversation_id>/`).
- **DO** construct a roadmap checklist file named `pr_review_tracking.md` or `pr_<PR>_comments.md` exclusively within your private Gemini App Data brain workspace.
- **DO NOT** write or track general developer checklists inside the official repository version control (Git).
- **DO** order checklists by complexity:
  1. **Tier 1 (Direct/Inline)**: Inline pointer changes, import blocks, compiler lint fixes.
  2. **Tier 2 (Medium Assertions)**: Integration tests, mock/stub adaptations.
  3. **Tier 3 (Complex Loops)**: Architectural adjustments, consensus logic, sync mechanisms.

---

## 2. Systematic Staged Execution

- **DO** execute changes sequentially, testing and resolving them tier-by-tier.
- **DO NOT** announce wins, declare unconditional success, or claim that comments are completely resolved if there are tasks or comments marked as **Postponed** or **Deferred** in the roadmap.
- **DO** explicitly call out all deferred items that require user alignment when presenting status updates.
- **DO** resync active review threads programmatically using the `gh` CLI before transitioning between major implementation phases.

### Roadmap Layout Example
```markdown
# PR #12345 Review Tracking
| ID | File | Line | Actionable Item | Status | Done? |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **3292537128** | `server/etcdserver/server.go` | 1405 | Use inline new allocations | ⏳ Pending | [ ] |
```
