# Repository Command Mappings (etcd)

This document maps abstract development workflows (testing, building, linting, formatting) to specific `etcd` repository commands. Use these commands when generic instructions in `SKILL.md` files refer to these workflows.

## Toolchain & Go Overrides
* **Environment Prefix**: All `make` commands must be prefixed with `GOROOT=""` (e.g., `GOROOT="" make build`) to guarantee Go cleanly resolves to the active workspace toolchain rather than falling back to host defaults.

## Validation & Linting
* **Full Validation Pipeline**: `GOROOT="" make verify && GOROOT="" make build && GOROOT="" make test-unit`
* **Style/Lint Check**: `GOROOT="" make verify-lint`
* **Mod Tidy Check**: `GOROOT="" make verify-mod-tidy`
* **Go Formatting Check**: `./hack/verify-gofmt.sh`
* **BOM Check**: `GOROOT="" make verify-bom`
* **YAML Lint Check**: `GOROOT="" make verify-yamllint`
* **Shell Lint Check**: `GOROOT="" make verify-shellws`

## Automatic Fixes
* **Fix Style/Lint**: `GOROOT="" make fix-lint`
* **Fix Mod Tidy**: `GOROOT="" make fix-mod-tidy`
* **Fix Go Formatting**: `./hack/update-gofmt.sh`
* **Fix BOM**: `GOROOT="" make fix-bom`
* **Fix YAML Lint**: `GOROOT="" make fix-yamllint`
* **Fix Shell Workspace**: `GOROOT="" make fix-shell-ws`
* *Note: Do not run global aggregate `make fix` if a specific subrule fails; target the specific fix command.*

## Testing & Deflaking
* **Unit Tests**: `GOROOT="" make test-unit`
* **Integration Tests**: `GOROOT="" make test-integration`
