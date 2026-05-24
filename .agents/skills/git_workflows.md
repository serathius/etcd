---
description: Git history reconstruction, interactive rebase strategy, branch pointer rebuilding, and bisect conflict remediation.
globs: "**/*"
alwaysApply: false
---

# Git Workflows & Branch Management Standards

These guidelines govern interactive rebasing, branch pointer rebuilding, bisect metadata remediation, and clean commit chain assembly to ensure highly reviewable, structured PR submissions.

---

## 1. Git History Reconstruction & Branch Pointer Rebuilding

During active development, Git history can become cluttered with checkpoint commits. Before sending a PR for review, **DO** structure the commit stack into a clean, reviewable sequence of logical changes with precise and descriptive commit messages.

When executing interactive rebases or manual history rewrites:

### Detached HEAD and Bisect Metadata Interference
- **DO NOT** struggle with `git bisect reset` if it hits checking-out conflicts due to active or stale bisect metadata (like `.git/BISECT_LOG`, `.git/BISECT_START`).
- **DO** manually remove all bisect metadata files `rm -f .git/BISECT_*` directly, then force checkout/reset to cleanly rebuild the branch.

### Rebuilding Sequential Commit Chains
When splitting or modifying specific commits within a multi-file PR:
1. **DO** hard-reset to the target base commit or checkout the parent commit directly.
2. **DO** amend the parent/target commit first (e.g., to update dependency tags or amend descriptions).
3. **DO** use a separate, temporary branch (e.g., `serathius/bump-raft-rebuilt`) to incrementally `git cherry-pick` the downstream commit hashes one by one.
4. **DO** switch to your main PR branch and hard-reset it to the temporary branch pointer once complete: `git reset --hard serathius/bump-raft-rebuilt`.
5. **DO** delete the temporary branch to maintain a pristine workspace.

### Handling Merge Conflicts During Stash Pop
When pop-stashing comments or edits onto a temporary rebuilt historical branch and encountering conflicts on downstream files:
- **DO** stage the target files that you explicitly intend to amend on the base commit: `git add <file>`.
- **DO** cleanly discard or restore any downstream conflicted files using `git restore --source=HEAD -- <file>` before amending the commit to prevent downstream changes from polluting your historical base commits.

### PR Comments History Integration Standard
- **DO NOT** push or commit code-review resolutions as a new "fixup" or "comment resolution" commit at the tail of your PR branch.
- **DO** amend the original target commit in the stack directly that introduced the modified file using the temporary branch cherry-pick technique above to preserve the original commit chain order.


---

## 2. Atomic Commits & Review Sign-off

- **DO** split your changes into the smallest logical, reviewable units possible.
- **DO** ensure that every intermediate commit leaves the repository in a fully runnable state that passes relevant tests. When unsure what tests to run, run all of them.
- **DO** finalize a complete draft by splitting your changes into logical commits using the `git commit --signoff` command once relevant tests pass.
- **DO NOT** bundle multiple unrelated refactoring steps or feature updates into a single massive, unsigned commit.

---

## 3. Dependency Bumps and Verification Cycles

When updating or upgrading external Go packages and submodules:

### Verify-to-Fix Correspondence
The repository [Makefile](file:///usr/local/google/home/siarkowicz/src/go.etcd.io/etcd/Makefile) maps specific checking and correction target pairs. Note that the overarching `verify` and `fix` commands are special parent rules that execute all their respective subrules:

* 👑 **Parent Commands (Aggregate of all subrules)**:
  * `verify` matches `fix` (runs all checking and fixing routines)

* 🛠️ **Targeted Subrules**:
  * `verify-mod-tidy` matches `fix-mod-tidy` (cleans and tidies go.mod/go.sum)
  * `verify-bom` matches `fix-bom` (regenerates licensing Bill of Materials)
  * `verify-lint` matches `fix-lint` (runs golangci-lint fixes)
  * `verify-yamllint` matches `fix-yamllint` (runs yamllint fixes)
  * `verify-shellws` matches `fix-shell-ws` (resolves tabulator spacing in shell scripts)

- **DO** run the equivalent fix command (`GOROOT="" make fix-<target>` or `GOROOT="" make fix`) immediately after modifying target files or dependencies.
- **DO NOT** postpone running the fix commands until the end of the branch commit chain. Always execute the fix targets and stage the updated configuration, checksums, or BOM modifications directly within the **first commit** that introduced the change, rather than adding separate fix commits at the tail of the PR. This ensures every individual commit inside the PR is structurally consistent, standalone, and reviewable.

---

## 4. Git Lock Contention Remediation

During parallel build testing, code generation script executions, or IDE analysis, git background processes can leave behind lock metadata files.

- **DO NOT** panic or loop endlessly if you see: `fatal: Unable to create '.../.git/index.lock': File exists`.
- **DO** immediately force-remove the stale lock file `rm -f .git/index.lock` and retry your git or cherry-pick operations.

