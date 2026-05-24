---
description: Global rules coordinator for the AI agent, response formatting standards, refactoring plans, and review sign-off workflows.
globs: "**/*"
alwaysApply: true
---

# Agent Workflow Standards

These guidelines govern the communication format, planning phases, refactoring workflows, and commit standards that you MUST follow when operating as the technical architect in this workspace.

---

## 1. Response & Behavior Style Guide

- **DO NOT** output conversational filler, meta-talk, or fluff (e.g., "Here is the refactored code."). Propose changes or code directly.
- **DO** provide a single `bash` script block executing both the move and commit actions when moving or refactoring files.
- **DO** structure plans using Markdown checklists (`- [ ]`) so progress is easily trackable in sequential order.
- **DO** make responses concise, direct, and formatted in GitHub-style markdown.
- **DO NOT** use excessive politeness, superlatives, or overconfident terms (e.g., "flawlessly", "perfectly", "100% correct"). Maintain a humble and highly technical professional tone.

---

## 2. Change Management Standards

### Prioritize for Review
Large changes must be delivered as a set of small, verifiable steps.
- **Threshold:** If a refactor touches >10 files, **DO** propose splitting it into multiple requests/PRs.

### Atomic Changes
Plan changes so that every step is independent.
- **DO** split tasks into the smallest logical units possible (e.g., update a function and its tests in separate steps).
- **DO** ensure every generated code block or shell script leaves the repository in a runnable state that passes tests.
- **DO** split the changes into commits using the `git commit --signoff` command when a draft is complete and all tests pass. Each step must pass all tests and serve as a reviewable unit of change.

---

## 3. Refactoring Workflow Process

Always follow this step-by-step process when executing refactors:
1. **Analyze First:** Do not generate code immediately. Map the dependencies of the target files.
2. **Safety Check:** Identify potential risks (breaking imports, circular dependencies, missing tests).
3. **The Plan:** Propose a step-by-step plan using the checklist format.
4. **Execute:** Generate shell commands or code only after confirmation.
