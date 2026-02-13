---
description: Update existing code — creates branch from Linear ticket, implements changes, opens PR to atlanhq/atlas-metastore
allowed-tools: [Bash, Read, Grep, Glob, Edit, Write, Task, AskUserQuestion, EnterPlanMode]
argument-hint: "[linear-ticket-id] [description-of-change]"
---

# Update Code

End-to-end workflow: understand the task from a Linear ticket, create a feature branch, implement code changes, commit, and open a PR against `atlanhq/atlas-metastore` (origin) — NOT the upstream Apache Atlas repo.

## Phase 0: Resolve Linear Ticket

### 0a: Get ticket ID

Parse `$ARGUMENTS` for a Linear ticket ID (pattern: `MS-\d+` or `METASTORE-\d+` or similar `TEAM-NUMBER`).

**If no ticket ID found in arguments**, use AskUserQuestion:
- Question: "What is the Linear ticket ID for this change? (e.g., MS-123)"
- Options:
  - "I'll provide one" — wait for the user to type it
  - "No ticket — proceed without one" — skip ticket context, ask for a branch name instead

### 0b: Fetch ticket details (if ticket ID available)

Get the Linear API key from `.claude/config.json`:
```bash
LINEAR_API_KEY=$(jq -r '.mcpServers.linear.env.LINEAR_API_KEY // empty' .claude/config.json)
if [ -z "$LINEAR_API_KEY" ]; then
  echo "Error: LINEAR_API_KEY not found in .claude/config.json"
  # Use AskUserQuestion to ask the user for the key or skip Linear integration
fi
```

Fetch the ticket and validate the response:
```bash
response=$(curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: $LINEAR_API_KEY" \
  -d '{"query": "{ issues(filter: { number: { eq: NUMBER }, team: { key: { eq: \"TEAM\" } } }) { nodes { id identifier title description state { name } assignee { name } priority labels { nodes { name } } parent { identifier title } comments { nodes { body user { name } createdAt } } } } }"}')

# Validate the response
if echo "$response" | jq -e '.errors' > /dev/null 2>&1; then
  echo "Linear API error: $(echo "$response" | jq -r '.errors[0].message')"
  # Inform the user and proceed without ticket context
fi
if ! echo "$response" | jq -e '.data.issues.nodes[0]' > /dev/null 2>&1; then
  echo "Ticket not found — proceeding without ticket context"
fi
```

Extract from the ticket:
- **Title** — used for PR title and branch name
- **Description** — requirements, acceptance criteria, context
- **Comments** — additional context, design decisions
- **Parent issue** — for epic/project context
- **Labels** — for categorization

### 0c: If no ticket — get context from user

If the user opted out of a ticket, use AskUserQuestion:
- Question: "Briefly describe what this change does (used for branch name and PR description)"

## Phase 1: Create Feature Branch

### 1a: Ensure clean working tree

```bash
git status --porcelain
```

If there are uncommitted changes, use AskUserQuestion:
- Question: "There are uncommitted changes on the current branch. How should I proceed?"
- Options:
  - "Stash changes" — `git stash push -m "auto-stash before update-code"`
  - "Continue anyway" — changes will carry over to the new branch
  - "Abort" — stop and let the user handle it

### 1b: Update master and create branch

```bash
if ! git fetch origin master; then
  echo "Error: Failed to fetch origin/master. Check network and remote configuration."
  # Abort — do not proceed without a valid base
fi
```

Generate branch name from ticket:
- **With ticket:** `{ticket-id-lowercase}-{short-slug}` (e.g., `ms-123-fix-glossary-qn`)
- **Without ticket:** ask user for a branch name via AskUserQuestion

```bash
if ! git checkout -b <branch-name> origin/master; then
  echo "Error: Failed to create branch. It may already exist."
  # Use AskUserQuestion: "Branch <name> already exists. Use it, pick a new name, or abort?"
fi
```

Confirm: "Created branch `<branch-name>` from `origin/master`."

## Phase 2: Understand the Change — Plan Mode

Enter plan mode using EnterPlanMode to design the implementation.

In plan mode:
1. **Analyze the ticket requirements** — break down what needs to change
2. **Explore the codebase** — use Glob, Grep, Read, and Task (Explore agents) to find relevant files
3. **Identify affected files** — list every file that needs modification
4. **Design the approach** — outline the specific code changes per file
5. **Consider side effects** — check callers, tests, downstream impacts
6. **Note testing strategy** — what tests need to be added or updated

Present the plan and wait for user approval via ExitPlanMode.

## Phase 3: Implement Changes

After plan approval, implement the changes:

1. **Edit files** using the Edit tool (prefer Edit over Write for existing files)
2. **Follow existing patterns** — match the code style and conventions of surrounding code
3. **Verify compilation** after each significant change — use the build command from `CLAUDE.md`:
   ```bash
   mvn compile -pl <module> -am -DskipTests -Drat.skip=true 2>&1 | tail -50
   ```
   Note: Refer to the "Quick Build Command" section in `CLAUDE.md` for the full command with `JAVA_HOME` and Maven path appropriate to the local environment.
4. If compilation fails, fix errors before proceeding

## Phase 4: Run Tests

Run relevant tests to verify the changes. Use the build commands from `CLAUDE.md` (with `JAVA_HOME` and Maven path for your environment):

```bash
# Build dependencies first (refer to CLAUDE.md for full command)
mvn install -pl <module> -am -DskipTests -Drat.skip=true

# Run specific tests
mvn test -pl <module> -Dtest=<TestClass> -Drat.skip=true
```

**If tests fail**, do NOT skip or proceed silently. Use AskUserQuestion:
- Question: "Tests failed. How should I proceed?"
- Options:
  - "Analyze and fix" — investigate failures and fix the code
  - "Show me the failures" — display the test output for the user to review
  - "Abort" — stop the workflow and let the user handle it

## Phase 5: Commit Changes

### 5a: Stage and review

```bash
git status
git diff --stat
```

**Before staging**, scan for potential secrets in the changes:
```bash
if git diff --cached --diff-filter=ACMR -p | grep -iE '(api[_-]?key|secret|password|token|credential|bearer)\s*[:=]'; then
  echo "WARNING: Potential secrets detected in changes"
  # Use AskUserQuestion: "Potential secrets found in diff. Review before staging?"
fi
```

Stage only the relevant files (never use `git add -A` or `git add .`):
```bash
git add <specific-files>
```

### 5b: Create commit

Write a clear, concise commit message:
- If ticket exists: prefix with ticket ID (e.g., `MS-123: Fix glossary QN generation`)
- Summarize the "why", not just the "what"
- Keep the first line under 72 characters

```bash
git commit -m "$(cat <<'EOF'
MS-XXX: Brief description of what changed and why

- Detail 1
- Detail 2

Co-Authored-By: Claude Code <noreply@anthropic.com>
EOF
)"
```

## Phase 6: Push and Create PR

### 6a: Push the branch

**IMPORTANT:** Push to `origin` (atlanhq/atlas-metastore), NOT `upstream` (apache/atlas).

```bash
git push -u origin <branch-name>
```

### 6b: Create the Pull Request

Construct the PR targeting `origin/master` on `atlanhq/atlas-metastore`:

```bash
gh pr create \
  --repo atlanhq/atlas-metastore \
  --base master \
  --title "<PR title>" \
  --body "$(cat <<'EOF'
## Summary

<2-4 bullet points describing what changed and why>

## Linear Ticket

[MS-XXX: Ticket Title](https://linear.app/atlan/issue/MS-XXX)

**Requirements from ticket:**
<Key acceptance criteria or requirements extracted from the ticket>

## Changes

<List of files changed with brief description of each change>

| File | Change |
|------|--------|
| `path/to/file.java` | Brief description |

## Testing

- [ ] Compilation verified (`mvn compile`)
- [ ] Unit tests pass
- [ ] Manually tested (if applicable)

## Test Plan

<Bulleted checklist of what to verify>

## Notes

<Any additional context, trade-offs, or follow-up items>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

**If no Linear ticket**, omit the "Linear Ticket" section.

### 6c: Display result

Print the PR URL and a summary:
```
PR created: <URL>
Branch: <branch-name>
Target: atlanhq/atlas-metastore (master)
Ticket: MS-XXX (if applicable)
```

## Phase 7: Post-PR Actions

After the PR is created, offer optional follow-up:

Use AskUserQuestion:
- Question: "PR is ready. Anything else?"
- Options:
  - "Run QA pipeline" — invoke the `/qa-pipeline` skill on the new PR
  - "Generate unit tests" — invoke the `/generate-unit-tests` skill
  - "Update Linear ticket" — transition ticket status and add PR link as a comment
  - "Done" — end the workflow

### If "Update Linear ticket" is selected:

Post a comment on the Linear ticket with the PR link:
```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: API_KEY_HERE" \
  -d '{
    "query": "mutation { commentCreate(input: { issueId: \"ISSUE_UUID\", body: \"PR opened: <PR_URL>\" }) { success } }"
  }'
```

And attach the PR URL to the ticket:
```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: API_KEY_HERE" \
  -d '{
    "query": "mutation { attachmentCreate(input: { issueId: \"ISSUE_UUID\", url: \"PR_URL\", title: \"GitHub PR\" }) { success } }"
  }'
```

## Important Reminders

- **Always target `atlanhq/atlas-metastore`** — this is our fork, not Apache Atlas
- **Always push to `origin`** — never push to `upstream`
- **Always use `origin/master` as base** for new branches
- **Never force push** — create new commits to fix issues
- **Never commit secrets** — check for `.env`, credentials, API keys before staging
- **Always verify compilation** before committing
- **Always enter plan mode** before implementing — get user approval on the approach first
- **Stage specific files** — never use `git add .` or `git add -A`
