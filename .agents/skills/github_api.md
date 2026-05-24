---
description: GitHub API integration, PR review comments extraction using gh CLI, and JQ query parsing workflows.
globs: ["**/*.json", "**/*.md"]
alwaysApply: false
---

# GitHub API Integration Standards

Use these standards to retrieve code reviews, timeline comments, and thread states programmatically from GitHub's APIs.

---

## 1. General Discussion vs Inline Review Extraction

When retrieving feedback programmatically using the GitHub CLI (`gh`) to coordinate PR review checklists, **DO** query **both** separate API endpoints to prevent pagination gaps or hidden conversations:

### Inline/Review Comments (Line-Specific)
```bash
gh api repos/OWNER/REPO/pulls/PR_NUMBER/comments --paginate --jq '.[] | {id: .id, path: .path, line: (.line // .original_line), author: .user.login, body: .body}'
```

### General Timeline/Discussion Comments
```bash
gh api repos/OWNER/REPO/issues/PR_NUMBER/comments --paginate --jq '.[] | {id: .id, author: .user.login, body: .body}'
```

---

## 2. Programmatic Thread Status Verification

- **DO NOT** attempt to use visual browser subagents or UI scrapers to check PR thread status.
- **DO** rely exclusively on the `/pulls/PR_NUMBER/comments` API endpoint. GitHub only returns active (unresolved) comments through this endpoint. Once a conversation thread is marked "Resolved," it is archived and automatically filtered out of this API response.
- **DO** confirm that a comment is still open by verifying that its ID or body remains present in the returned active pulls comments list. If it is missing, it can be safely treated as resolved.
