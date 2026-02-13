#!/usr/bin/env python3
"""
Claude PR Review Script using LiteLLM Proxy
Posts inline review comments on PR code + a structured summary comment.
The LLM outputs the review directly as human-readable markdown (not JSON).
Inline comment data is embedded as a hidden HTML block at the end of the response.
Shows live progress on the PR by editing a single comment as tasks complete.
"""

import os
import re
import sys
import json
import time
import argparse
import subprocess
import requests
from typing import Optional, Dict, Any, List

from litellm_client import LiteLLMClient

# Delimiter used by the LLM to separate the visible markdown from inline comment data
INLINE_COMMENTS_DELIMITER = "<!-- INLINE_COMMENTS_JSON"


class GitHubAPI:
    """GitHub API client for PR operations"""

    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = "https://api.github.com"

    def get_pr_diff(self, pr_number: int) -> str:
        env = {**os.environ, "GH_TOKEN": self.token}
        result = subprocess.run(
            ["gh", "pr", "diff", str(pr_number)],
            capture_output=True, text=True, check=True, env=env
        )
        return result.stdout

    def get_pr_details(self, pr_number: int) -> Dict[str, Any]:
        """Get full PR details including body/description."""
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}"
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_pr_files(self, pr_number: int) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}/files"
        all_files = []
        page = 1
        while True:
            response = requests.get(
                url, headers=self.headers, timeout=30,
                params={"per_page": 100, "page": page}
            )
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            all_files.extend(batch)
            page += 1
        return all_files

    def post_pr_comment(self, pr_number: int, body: str) -> Dict[str, Any]:
        url = f"{self.base_url}/repos/{self.repo}/issues/{pr_number}/comments"
        response = requests.post(
            url, headers=self.headers, json={"body": body}, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def update_pr_comment(self, comment_id: int, body: str) -> Dict[str, Any]:
        url = f"{self.base_url}/repos/{self.repo}/issues/comments/{comment_id}"
        response = requests.patch(
            url, headers=self.headers, json={"body": body}, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def submit_review(self, pr_number: int, commit_id: str, body: str,
                      comments: List[Dict[str, Any]], event: str = "COMMENT"):
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}/reviews"
        payload = {
            "commit_id": commit_id,
            "body": body,
            "event": event,
            "comments": comments
        }
        response = requests.post(
            url, headers=self.headers, json=payload, timeout=60
        )
        response.raise_for_status()
        return response.json()


# ---------------------------------------------------------------------------
# Live progress tracker
# ---------------------------------------------------------------------------

class ProgressTracker:
    TASKS = [
        "Fetch PR diff, description, and changed files",
        "Analyze code with Claude AI",
        "Post inline review comments",
        "Compile review summary",
    ]

    def __init__(self, github_api: GitHubAPI, pr_number: int, pr_title: str):
        self.github_api = github_api
        self.pr_number = pr_number
        self.pr_title = pr_title
        self.comment_id: Optional[int] = None
        self.completed: List[int] = []
        self.start_time = time.time()

    def _build_progress_body(self, active_task: Optional[int] = None) -> str:
        lines = [
            f"## 🤖 Claude AI Review — PR #{self.pr_number}",
            "",
            f"⏳ **Reviewing:** {self.pr_title}",
            "",
            "### Tasks",
        ]
        for i, task in enumerate(self.TASKS):
            if i in self.completed:
                lines.append(f"- [x] {task}")
            elif i == active_task:
                lines.append(f"- [ ] ⏳ {task}...")
            else:
                lines.append(f"- [ ] {task}")

        elapsed = int(time.time() - self.start_time)
        lines.extend(["", f"*Running for {elapsed}s...*", "", "---",
                       "*Powered by Claude via LiteLLM Proxy*"])
        return "\n".join(lines)

    def start(self):
        body = self._build_progress_body(active_task=0)
        try:
            result = self.github_api.post_pr_comment(self.pr_number, body)
            self.comment_id = result["id"]
            print(f"Progress comment posted (id={self.comment_id})")
        except Exception as e:
            print(f"Warning: could not post progress comment: {e}")

    def complete_task(self, task_index: int, next_task: Optional[int] = None):
        self.completed.append(task_index)
        if self.comment_id is None:
            return
        body = self._build_progress_body(active_task=next_task)
        try:
            self.github_api.update_pr_comment(self.comment_id, body)
        except Exception as e:
            print(f"Warning: could not update progress comment: {e}")

    def finish(self, final_body: str):
        elapsed = int(time.time() - self.start_time)
        header = f"**Claude AI finished reviewing in {elapsed}s**\n\n---\n\n"
        full_body = header + final_body
        if self.comment_id is None:
            try:
                self.github_api.post_pr_comment(self.pr_number, full_body)
            except Exception as e:
                print(f"Failed to post final comment: {e}")
                sys.exit(1)
        else:
            try:
                self.github_api.update_pr_comment(self.comment_id, full_body)
                print("Progress comment updated with final review.")
            except Exception as e:
                print(f"Warning: could not update, posting new: {e}")
                try:
                    self.github_api.post_pr_comment(self.pr_number, full_body)
                except Exception as e2:
                    print(f"Failed to post final comment: {e2}")
                    sys.exit(1)

    def fail(self, error_message: str):
        elapsed = int(time.time() - self.start_time)
        lines = [
            f"## 🤖 Claude AI Review — PR #{self.pr_number}", "",
            f"❌ **Review failed after {elapsed}s**", "", "### Tasks",
        ]
        for i, task in enumerate(self.TASKS):
            lines.append(f"- [x] {task}" if i in self.completed else f"- [ ] {task}")
        lines.extend(["", f"**Error:** {error_message}", "", "---",
                       "*Powered by Claude via LiteLLM Proxy*"])
        if self.comment_id:
            try:
                self.github_api.update_pr_comment(self.comment_id, "\n".join(lines))
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Diff-line mapping
# ---------------------------------------------------------------------------

def build_diff_line_map(files: List[Dict[str, Any]]) -> Dict[str, Dict[int, int]]:
    mapping: Dict[str, Dict[int, int]] = {}
    for f in files:
        patch = f.get("patch", "")
        if not patch:
            continue
        file_path = f["filename"]
        line_map: Dict[int, int] = {}
        position = 0
        new_line = 0
        for raw_line in patch.split("\n"):
            if raw_line.startswith("@@"):
                try:
                    plus_part = raw_line.split("+")[1].split("@@")[0]
                    new_line = int(plus_part.split(",")[0]) - 1
                except (IndexError, ValueError):
                    pass
                position += 1
                continue
            position += 1
            if raw_line.startswith("-"):
                continue
            new_line += 1
            if raw_line.startswith("+") or not raw_line.startswith("-"):
                line_map[new_line] = position
        mapping[file_path] = line_map
    return mapping


# ---------------------------------------------------------------------------
# Prompt — LLM outputs markdown directly
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a senior staff engineer doing a thorough code review for Apache Atlas
Metastore — a Java 17 / Maven project (JanusGraph, Cassandra, Elasticsearch, Kafka).

Project layout: `repository/` (core logic, preprocessors), `webapp/` (REST),
`intg/` (API models), `helm/` (charts), `.github/` (CI/CD).

Your review must be **exhaustive and precise**. For every finding you MUST:
1. Identify the exact file and line.
2. Show the vulnerable/problematic code snippet.
3. Explain WHY it is a problem and what the real-world IMPACT is.
4. Provide a concrete fix with code.

Categories to check:
- **Bugs** — NPEs, resource leaks, race conditions, unclosed transactions
- **Security** — injection, missing auth, leaked secrets, input validation
- **Performance** — N+1 queries, hot-path allocations, missing caching
- **Compatibility** — REST contract changes, TypeDef changes, Kafka format
- **Graph DB** — JanusGraph tx handling, index usage, vertex/edge patterns

Rules:
- ONLY flag real problems — never comment on style or formatting.
- If the PR is clean, say so clearly — do not invent issues.
- Quality over quantity."""


def build_review_prompt(pr_number: int, pr_title: str, pr_author: str,
                        pr_description: str, file_list: List[str],
                        diff: str) -> str:
    return f"""Review this PR and write your review directly as a **well-formatted GitHub markdown document**.
Do NOT return JSON. Write the review as you would post it on GitHub — human-readable, with rich formatting.

## PR
- **#{pr_number}** — {pr_title}
- **Author:** {pr_author}
- **Changed files:** {', '.join(file_list) if file_list else 'N/A'}

## PR Description (written by the author)
```
{pr_description[:5000] if pr_description else 'No description provided.'}
```

## Diff
```diff
{diff}
```

---

## Output — write the ENTIRE review as markdown, following this EXACT structure:

# 🤖 Claude AI Code Review
**PR #{pr_number}** — {pr_title}
**Author:** @{pr_author}

### ✅ Tasks Completed
- [x] Fetch PR diff, description, and changed files
- [x] Analyze code with Claude AI
- [x] Post inline review comments
- [x] Compile review summary

---
### 📋 Summary
[2-3 sentence overview of what the PR does and overall quality]

> **📝 PR Description Check:** [1-2 sentences: does the PR description accurately reflect the actual code changes? Flag mismatches, missing context, or misleading claims.]

| Category | Count |
|----------|-------|
| 🔴 Critical | [N] |
| 🔒 Security | [N] |
| 💡 Recommendations | [N] |
| **Total** | **[N]** |

---
### 🔴 Critical Findings

[If none: "No critical issues found. ✅"]

[For each finding, use this format:]

#### [N]. [Short descriptive title]
📍 **`file/path.java:line`**
[If duplicated: ↳ Also present in: `file2:line`, `file3:line`]

**Vulnerable code:**
```java
[exact code copied from the diff, 1-5 lines]
```
**Why this is critical:** [2-3 sentences: what is wrong, real-world impact]

**Example of misuse:** [1-2 sentences: how this can be exploited or how it fails]

**Required Fix:**

<details><summary><b>Option A: [name]</b></summary>

```java
[fixed code]
```
[1 sentence why this works]

</details>

<details><summary><b>Option B: [name]</b></summary>

```java
[fixed code]
```
[1 sentence why this works]

</details>

---
### 🔒 Security Issues & Fixes

[If none: "No security issues found. ✅"]

[For each finding:]

#### [N]. [Title]
📍 **`file/path.java:line`**
[If duplicated: ↳ Also present in: `file2:line`]

**Vulnerable code:**
```java
[exact code from diff]
```
**Issue:** [what the security issue is and its impact]

**Fix:**
```java
[fixed code]
```
_[Why this fixes it]_

---
### 💡 Recommendations

[If none: omit this entire section]

[For each:]

#### [N]. [Title]
📍 **`file/path.java:line`**
[If duplicated: ↳ Also in: `file2:line`]

**Current code:**
```java
[current code from diff]
```
[What could be improved and why]

**Suggested improvement:**
```java
[improved code]
```

---
### ⚡ Optimisations

[If none: omit this entire section]

- [one-liner performance/quality improvement suggestion]
- [...]

---
### [emoji] Verdict: **[APPROVE / APPROVE WITH COMMENTS / REQUEST CHANGES]**

[One sentence justification]

### 📌 Next Steps

1. [actionable next step]
2. [...]

---
*Powered by Claude via LiteLLM Proxy*

---

## CRITICAL — Inline Comments Block

After the `*Powered by Claude via LiteLLM Proxy*` line, you MUST append a hidden HTML comment block
containing JSON data for inline PR comments. This block is NOT displayed on GitHub but is parsed
by the script to post inline comments on the actual code lines.

Format — EXACTLY:
```
<!-- INLINE_COMMENTS_JSON
[
  {{
    "file": "exact/file/path.java",
    "line": 42,
    "severity": "critical|security|recommendation",
    "title": "Short title",
    "comment": "Markdown body for the inline comment. Include the issue description and a suggested fix code block."
  }}
]
-->
```

Rules for the inline comments block:
- `file` must be an EXACT path from the changed files list.
- `line` must exist in the NEW file (right side of diff, `+` or context lines).
- `severity` must be one of: `critical`, `security`, `recommendation`.
- `comment` is posted directly as an inline comment on the PR code — make it self-contained.
  Include the issue + fix code block so the developer sees the full context on that line.
- Every finding from Critical Findings, Security Issues, and Recommendations MUST have
  a matching entry in this block.
- Maximum: 5 critical, 5 security, 10 recommendations.
- If the PR is clean with zero findings, output an empty array: `[]`
"""


# ---------------------------------------------------------------------------
# Response parser — split markdown from inline comments
# ---------------------------------------------------------------------------

def parse_llm_response(raw_response: str):
    """Split LLM response into visible markdown and inline comment data.

    Returns:
        (markdown_body, inline_comments_list)
    """
    # Split at the hidden HTML comment delimiter
    if INLINE_COMMENTS_DELIMITER in raw_response:
        parts = raw_response.split(INLINE_COMMENTS_DELIMITER, 1)
        markdown_body = parts[0].rstrip()
        # Extract JSON from the HTML comment block: <!-- INLINE_COMMENTS_JSON [...] -->
        json_block = parts[1]
        # Remove closing -->
        json_block = re.sub(r'-->\s*$', '', json_block).strip()
        try:
            inline_comments = json.loads(json_block)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Warning: could not parse inline comments JSON: {e}")
            print(f"Block (first 300): {json_block[:300]}")
            inline_comments = []
    else:
        print("Warning: no INLINE_COMMENTS_JSON block found in LLM response.")
        markdown_body = raw_response.strip()
        inline_comments = []

    return markdown_body, inline_comments


# ---------------------------------------------------------------------------
# Main review logic
# ---------------------------------------------------------------------------

def review_pr(llm_client: LiteLLMClient, github_api: GitHubAPI,
              pr_number: int, pr_title: str, pr_author: str):

    print(f"Reviewing PR #{pr_number}: {pr_title}")
    print(f"Author: {pr_author}")

    progress = ProgressTracker(github_api, pr_number, pr_title)
    progress.start()

    # Task 0: Fetch PR data
    print("Fetching PR details...")
    try:
        pr_details = github_api.get_pr_details(pr_number)
        pr_description = pr_details.get("body", "") or ""
        head_sha = pr_details["head"]["sha"]
    except Exception as e:
        print(f"Could not fetch PR details: {e}")
        pr_description = ""
        head_sha = None

    print("Fetching PR diff...")
    try:
        diff = github_api.get_pr_diff(pr_number)
    except Exception as e:
        progress.fail(f"Failed to get PR diff: {e}")
        sys.exit(1)

    if len(diff) > 50000:
        diff = diff[:50000] + "\n\n[... diff truncated due to size ...]"
        print("Diff truncated (>50 KB)")

    print("Fetching changed files...")
    try:
        files = github_api.get_pr_files(pr_number)
        file_list = [f["filename"] for f in files]
        print(f"Changed files ({len(file_list)}): {', '.join(file_list[:10])}")
    except Exception as e:
        print(f"Could not fetch files: {e}")
        files = []
        file_list = []

    diff_line_map = build_diff_line_map(files)
    progress.complete_task(0, next_task=1)

    # Task 1: Analyze with LLM — returns markdown directly
    print("Analyzing PR with Claude via LiteLLM...")
    review_prompt = build_review_prompt(
        pr_number, pr_title, pr_author, pr_description, file_list, diff
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": review_prompt}
    ]

    try:
        raw_response = llm_client.chat(messages, max_tokens=4096, temperature=0.1)
    except Exception as e:
        progress.fail(f"LLM analysis failed: {e}")
        sys.exit(1)

    # Parse: split markdown body from hidden inline comments JSON
    markdown_body, inline_comments = parse_llm_response(raw_response)
    print(f"LLM returned {len(inline_comments)} inline comment(s)")
    progress.complete_task(1, next_task=2)

    # Task 2: Post inline comments
    inline_posted = 0
    inline_failed = 0
    review_comments = []

    if head_sha and inline_comments:
        for ic in inline_comments:
            file_path = ic.get("file", "")
            line = ic.get("line", 0)
            position = diff_line_map.get(file_path, {}).get(line)
            if not position:
                inline_failed += 1
                continue

            severity = ic.get("severity", "recommendation")
            sev_icon = {"critical": "🔴", "security": "🔒"}.get(severity, "💡")
            sev_label = {"critical": "Critical", "security": "Security"}.get(severity, "Recommendation")

            title = ic.get("title", "Issue")
            comment_body = ic.get("comment", "")

            body = f"{sev_icon} **{sev_label}: {title}**\n\n{comment_body}"

            review_comments.append({
                "path": file_path,
                "position": position,
                "body": body
            })

        if review_comments:
            try:
                # Determine review event from the markdown verdict line
                event = "COMMENT"
                if "REQUEST CHANGES" in markdown_body.upper():
                    event = "REQUEST_CHANGES"
                elif "### ✅ Verdict" in markdown_body or "Verdict: **APPROVE**" in markdown_body:
                    event = "APPROVE"

                github_api.submit_review(
                    pr_number, head_sha, body="",
                    comments=review_comments, event=event
                )
                inline_posted = len(review_comments)
                print(f"Posted {inline_posted} inline comment(s).")
            except Exception as e:
                print(f"Failed to submit inline review: {e}")
                inline_failed += len(review_comments)
                inline_posted = 0

    progress.complete_task(2, next_task=3)

    # Task 3: Post the review summary (the markdown body from the LLM)
    print("Posting review summary...")

    # Append inline comment stats if any were posted/failed
    if inline_posted > 0 or inline_failed > 0:
        stats_lines = []
        if inline_posted > 0:
            stats_lines.append(f"> 💬 **{inline_posted}** inline comment(s) posted directly on code")
        if inline_failed > 0:
            stats_lines.append(f"> ⚠️ **{inline_failed}** comment(s) could not be mapped to diff lines")
        # Insert after the stats table (before the first ---)
        insert_marker = "---\n### 🔴 Critical Findings"
        if insert_marker in markdown_body:
            markdown_body = markdown_body.replace(
                insert_marker,
                "\n".join(stats_lines) + "\n\n" + insert_marker
            )
        else:
            markdown_body += "\n\n" + "\n".join(stats_lines)

    progress.finish(markdown_body)
    print("Review posted successfully!")


def main():
    parser = argparse.ArgumentParser(description="Claude PR Review via LiteLLM")
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--pr-title", required=True)
    parser.add_argument("--pr-author", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--litellm-url", default="https://llmproxy.atlan.dev/v1")
    parser.add_argument("--litellm-key", default=None)
    parser.add_argument("--github-token", default=None)
    parser.add_argument("--model", default="claude")

    args = parser.parse_args()

    litellm_key = args.litellm_key or os.environ.get("LITELLM_API_KEY")
    github_token = args.github_token or os.environ.get("GITHUB_TOKEN")

    if not litellm_key:
        print("Error: LiteLLM API key required (--litellm-key or LITELLM_API_KEY env).")
        sys.exit(1)
    if not github_token:
        print("Error: GitHub token required (--github-token or GITHUB_TOKEN env).")
        sys.exit(1)

    llm_client = LiteLLMClient(args.litellm_url, litellm_key, args.model)
    github_api = GitHubAPI(github_token, args.repo)
    review_pr(llm_client, github_api, args.pr_number, args.pr_title, args.pr_author)


if __name__ == "__main__":
    main()
