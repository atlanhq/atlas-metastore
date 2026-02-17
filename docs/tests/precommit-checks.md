# Pre-commit Checks

Pre-commit hooks run automatically on every `git commit` to catch issues before code is pushed.

## Setup (one-time)

```bash
brew install pre-commit gitleaks
pre-commit install
```

## Hooks

| Hook | What it does |
|------|-------------|
| **gitleaks** | Scans staged files for secrets, passwords, API keys |
| **check-merge-conflict** | Catches leftover merge conflict markers |
| **fix-byte-order-marker** | Removes UTF-8 BOM from files |
| **check-added-large-files** | Blocks files larger than 500KB |
| **check-case-conflict** | Catches filename case collisions |

## Running manually

```bash
# Run on all files
pre-commit run --all-files

# Run a specific hook
pre-commit run gitleaks
```

## Troubleshooting

**"gitleaks" command not found** — Install it: `brew install gitleaks`

**False positive from gitleaks** — Add to `.gitleaksignore` or skip once with `git commit --no-verify`

**Update hooks to latest** — `pre-commit autoupdate`
