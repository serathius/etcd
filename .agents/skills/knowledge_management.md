---
description: Lifecycle rules for creating new modular skills, trigger identification, and guidelines for formatting instructions.
globs: ".agents/**/*"
alwaysApply: false
---

# Knowledge & Skill Management Standards

These standards govern the metadata schema, triggers, and lifecycle workflows that you MUST use to create, refactor, and retire modular AI skills to keep the repository's instruction set ultra-focused and context-lean.

---

## 1. The Layered Architecture (Rules vs. Skills vs. Workflows)

To prevent prompt context bloat and keep instruction recall highly optimized, you MUST structure instructions across three distinct layers:

1. **Universal Rules (`AGENTS.md` / `GEMINI.md`):**
   * Contains **always-on, passive constraints** (e.g., directory architecture rules, naming conventions, commenting bans).
   * Use `AGENTS.md` in the workspace root for **cross-tool compatibility** (Cursor, Claude Code, Antigravity).
   * Use `GEMINI.md` for **Antigravity-specific settings** or tool overrides. Settings in `GEMINI.md` override matching keys in `AGENTS.md`.
2. **Specialized Skills (`SKILL.md`):**
   * Contains **on-demand, task-specific instruction sets** (e.g., how to write a benchmark, how to deflake a test, how to run failpoints).
   * These are stored inside modular subdirectories under `.agents/skills/`.
   * These are **only loaded into the agent's context** when the agent actively matches the semantic trigger.
3. **Active Workflows:**
   * Interactive or multi-step scripts or CLI triggers that the developer or agent executes for heavy-lifting actions (e.g., bisecting, running stress tests).

---

## 2. Identifying Skill Triggers

You must proactively create or refactor skill files when any of the following triggers are met:

1. **The Repetitive Mistake Trigger:**
   * If you make the same mechanical or architectural mistake twice (e.g., compile version mismatch or lock copying), **DO** extract a skill rule detailing the correct implementation pattern to prevent it from happening a third time.
2. **The Command Clutter Trigger:**
   * If a complex CLI command chain (e.g., specific stress test options, API filtering via `gh/jq`) is run successfully, **DO** document the exact command syntax as a reusable skill instead of forcing future agent sessions to regenerate it.
3. **The "Opaque PDF" or Transcript Learning Trigger:**
   * When the user shares high-value external knowledge (e.g., a benchmarking video transcript or debugging log), **DO** instantly translate it into an active skill block rather than storing it as a passive conversational text.
4. **The File Type Divergence Trigger:**
   * If an existing skill file covers rules applying to completely different file types (e.g., mixing generic testing with VCS git configs), **DO** split them immediately to ensure specific rules only load on relevant globs.

---

## 3. Modular Skill Design Standards

Every skill file you create must strictly adhere to the following layout:

### YAML Frontmatter Header
Every skill file must begin with YAML Frontmatter detailing:
- `description`: **CRITICAL.** Write a highly detailed, semantic trigger phrase describing exactly when this skill is needed. The agent's router matches this text to determine when to load the skill.
- `globs`: A list of target file globs (e.g., `**/*_test.go`, `Makefile`) so the rule is loaded only when editing matching files.
- `alwaysApply`: Explicitly set to `false` for skills to avoid wasting context.

### Structure of SKILL.md
- **YAML Frontmatter** (as detailed above).
- **# Goal**: A concise statement of the capability.
- **# Instructions**: Step-by-step logic, assertions, rules, and warnings using highly assertive, positive directives ("Do this", "Ensure that"). Do NOT use passive or hypothetical language ("could", "should").
- **# Correct vs. Incorrect Patterns**: Clear, concise code/syntax snippets showing the correct vs. bad implementation.

---

## 4. Integration & Indexing Workflow

When creating a new skill:
1. **Write the Skill**: Create the new `.md` file inside `.agents/skills/` or `.agents/standards/`.
2. **Prune the Old**: If extracting from an existing file, remove the original rules to prevent duplicate context weights.
3. **Index**: Register the new skill inside the `📂 Modular Standards & Guidelines Directory` in [AGENTS.md](file:///usr/local/google/home/siarkowicz/src/go.etcd.io/etcd/AGENTS.md) using standard links.
4. **Never Delete/Prune Knowledge (The Preservation Invariant)**:
   * **DO NOT** delete or permanently discard any existing technical rule, command sequence, or historical issue case from the skill bank without confirmation.
   * If a rule, CLI target, or checklist is redundant or deprecated, **DO** move it to a temporary review archive file (e.g., `.agents/skills/archived_rules.md`) for explicit user confirmation first. Always protect the historical bank of knowledge.
5. **Local-Only Instruction Files Invariant**:
   * **DO NOT** under any circumstances add, track, or commit `GEMINI.md`, `AGENTS.md`, or any files in the `.gemini/` or `.agents/` directory to the version control repository (Git). These instruction sets must remain strictly local and private to the developer's workspace environment.

---

## 5. Continuous Skill Optimization Invariant

- **DO** periodically update and refine this meta-skills guide by querying the web for emerging, cutting-edge best practices regarding modular system prompt instructions (e.g., MDC, `.cursorrules` or `.clinerules` standards).
- **DO** treat this `knowledge_management.md` file as the baseline foundation to audit, clean, refactor, and improve all other rule files inside the `.agents/` folder and [AGENTS.md](file:///usr/local/google/home/siarkowicz/src/go.etcd.io/etcd/AGENTS.md) systematically.
- **DO** perform a comprehensive audit and refactoring pass across the entire ruleset whenever this meta-skill guide receives major structural enhancements.
- **DO** write, update, and clean all skill files directly, no need to wait for user to confirm, they can read the code.
- **DO propagate general engineering invariants:** Whenever a general repository-wide engineering standard is established (e.g., requiring pre-change baselines, quantitative post-change impact verification, or strict hypothesis rejection rules), you **MUST** propagate and tailor these instructions across all specific files in the `.agents/skills/` directory (e.g., `deflaking_guidelines.md`, `benchmarking_guidelines.md`, `testing_guidelines.md`) to ensure it is enforced everywhere.
