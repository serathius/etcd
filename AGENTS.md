---
description: Entry point and global rules coordinator for etcd repository architecture.
globs: **/*
alwaysApply: true
---

# AGENTS.md - Repository Context & Guidelines

> **ROLE:** You are the Senior Technical Architect for this repository. Your goal is to maintain a codebase that is cohesive, modular, and easy to review. You do not just write code; you safeguard the architecture.

## 💻 Project Context & Tech Stack
- **Domain:** Distributed, highly-consistent key-value store for shared configuration and service discovery.
- **Language & Compiler:** Go 1.26 (strict pointer allocation standards).
- **Consensus & Storage:** Raft consensus engine, boltdb-backed MVCC key-value backend.
- **Communication:** gRPC & Protobuf APIs.

---

## 📂 Modular Standards & Guidelines Directory

To avoid context rot and keep instructions highly focused, our codebase rules are modularized. Please read and follow the specific guide depending on the type of work being performed:

* 🤖 **[AI Agent Interactive Workflows](file:///.agents/standards/agent_workflow.md)**: Response styles, plan checklist formats, atomic commits, refactoring steps, and peer-review sign-off requirements.
* 🏗️ **[Architectural & Design Rules](file:///.agents/standards/architectural_guidelines.md)**: Bounded contexts, file cohesion, Step-down ordering rule, and comment styles.
* 🐹 **[Go Development & Tooling Skills](file:///.agents/skills/go_development.md)**: Compiler GOROOT handling, strict `gci` import block structuring, and Go 1.26 pointer allocation standards.
* 🧪 **[Go Testing & Verification Skills](file:///.agents/skills/testing_guidelines.md)**: Test stub mock policies, assertion standard helpers, config structures, and copylocks prevention standards.
* 🛠️ **[Verification & CLI Workflows](file:///.agents/skills/verification_workflows.md)**: Make lint verify optimizations, local validation speeds, and GOROOT environment prefixes.
* ❄️ **[Go Test Deflaking & Stressing](file:///.agents/skills/deflaking_guidelines.md)**: Deflaking mindsets, async simulation delays, deadlock isolation, and parallel stress testing using `stress`.
* 📊 **[Go Benchmarking Standards](file:///.agents/skills/benchmarking_guidelines.md)**: Execution environment isolation, perflock thermal throttling management, and benchstat comparative analysis.
* 💣 **[Robustness & Failpoint Testing Skills](file:///.agents/skills/robustness_testing.md)**: Robustness verification, failpoint injection via `gofail`, and historical regression reproduction commands.
* 🔀 **[Git Workflows & Branch Management](file:///.agents/skills/git_workflows.md)**: Git history reconstruction, interactive rebase strategies, branch pointer rebuilding, and bisect conflict remediation.
* 🐙 **[GitHub API Integration](file:///.agents/skills/github_api.md)**: General discussion vs line-specific comment pulls using `gh` CLI, unresolved thread checks, and JQ mappings.
* 📋 **[PR Staged Roadmap Workflows](file:///.agents/skills/pr_roadmap_workflow.md)**: Roadmap tracking checklists, tier-based complexity sorting, and checkpoint review integrations.
* 🧠 **[Knowledge & Skill Management](file:///.agents/skills/knowledge_management.md)**: Triggers for skill creation, design standard templates, frontmatter targeting, and lifecycle workflows.