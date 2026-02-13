#!/usr/bin/env python3
"""
Claude PR Review Script using LiteLLM Proxy
Posts inline review comments on PR code + a structured summary comment.
Shows live progress on the PR by editing a single comment as tasks complete.
"""

import os
import sys
import json
import time
import argparse
import subprocess
import requests
from typing import Optional, Dict, Any, List

from litellm_client import LiteLLMClient


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
# Prompts
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
- If the PR is clean, return empty arrays.
- Quality over quantity."""


def build_findings_prompt(pr_number: int, pr_title: str, pr_author: str,
                          pr_description: str, file_list: List[str],
                          diff: str) -> str:
    return f"""Review this PR and return your findings as **pure JSON** (no markdown fences).

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

## Response Schema — follow EXACTLY

{{
  "summary": "<2-3 sentence overview of what the PR does and overall quality>",
  "description_vs_code": "<1-2 sentences: does the PR description accurately reflect the actual code changes? Flag any mismatches, missing context, or misleading claims.>",
  "verdict": "APPROVE | APPROVE_WITH_COMMENTS | REQUEST_CHANGES",
  "verdict_reason": "<one sentence justification>",
  "critical_findings": [
    {{
      "title": "<short descriptive title for this vulnerability/bug>",
      "file": "<exact file path from changed files list>",
      "line": <line number in the NEW file>,
      "code_snippet": "<the exact vulnerable/buggy code — copy from the diff, 1-5 lines>",
      "why_critical": "<2-3 sentences: what is wrong, what is the real-world impact>",
      "exploit_example": "<1-2 sentences: how could this be misused or how does it fail>",
      "duplicates": ["<file2:line>", "<file3:line>"],
      "fixes": [
        {{
          "option": "<Option A / Option B / etc>",
          "code": "<fixed code snippet>",
          "explanation": "<1 sentence why this works>"
        }}
      ]
    }}
  ],
  "security_findings": [
    {{
      "title": "<title>",
      "file": "<file path>",
      "line": <line>,
      "code_snippet": "<vulnerable code>",
      "description": "<what the security issue is and its impact>",
      "duplicates": ["<file2:line>"],
      "fix_code": "<fixed code>",
      "fix_explanation": "<why this fixes it>"
    }}
  ],
  "recommendations": [
    {{
      "title": "<title>",
      "file": "<file path>",
      "line": <line>,
      "code_snippet": "<current code>",
      "description": "<what could be improved and why>",
      "suggested_code": "<improved code>",
      "duplicates": ["<file2:line>"]
    }}
  ],
  "optimisations": [
    "<one-liner performance/quality improvement suggestion>"
  ],
  "next_steps": [
    "<actionable next step for the PR author>"
  ]
}}

### Rules:
- `file` must be an EXACT path from the changed files list.
- `line` must exist in the NEW file (right side of diff, `+` or context lines).
- `code_snippet` must be copied verbatim from the diff — do NOT paraphrase.
- `duplicates` lists OTHER locations with the same issue (format: `file:line`). Empty array if unique.
- For critical_findings, provide at least one fix option with working code.
- Read the PR description carefully and validate it against the actual code changes.
- Maximum: 5 critical, 5 security, 10 recommendations.
"""


# ---------------------------------------------------------------------------
# Markdown builder
# ---------------------------------------------------------------------------

def build_summary_markdown(review_data: Dict, pr_number: int, pr_title: str,
                           pr_author: str, inline_posted: int,
                           inline_failed: int) -> str:
    """Build the full review comment from structured JSON."""

    summary = review_data.get("summary", "No summary provided.")
    desc_check = review_data.get("description_vs_code", "")
    verdict = review_data.get("verdict", "APPROVE_WITH_COMMENTS")
    verdict_reason = review_data.get("verdict_reason", "")
    critical = review_data.get("critical_findings", [])
    security = review_data.get("security_findings", [])
    recs = review_data.get("recommendations", [])
    optimisations = review_data.get("optimisations", [])
    next_steps = review_data.get("next_steps", [])

    verdict_emoji = {"APPROVE": "✅", "APPROVE_WITH_COMMENTS": "⚠️",
                     "REQUEST_CHANGES": "❌"}.get(verdict, "ℹ️")

    total = len(critical) + len(security) + len(recs)
    L = []  # output lines

    # ── Title ──
    L.append(f"# 🤖 Claude AI Code Review")
    L.append(f"**PR #{pr_number}** — {pr_title}  ")
    L.append(f"**Author:** @{pr_author}\n")

    # ── Tasks ──
    L.append("### ✅ Tasks Completed")
    L.append("- [x] Fetch PR diff, description, and changed files")
    L.append("- [x] Analyze code with Claude AI")
    L.append("- [x] Post inline review comments")
    L.append("- [x] Compile review summary\n")

    # ── Summary ──
    L.append("---")
    L.append("### 📋 Summary\n")
    L.append(f"{summary}\n")

    if desc_check:
        L.append(f"> **📝 PR Description Check:** {desc_check}\n")

    # ── Stats ──
    L.append(f"| Category | Count |")
    L.append(f"|----------|-------|")
    L.append(f"| 🔴 Critical | {len(critical)} |")
    L.append(f"| 🔒 Security | {len(security)} |")
    L.append(f"| 💡 Recommendations | {len(recs)} |")
    L.append(f"| **Total** | **{total}** |")
    L.append("")
    if inline_posted > 0:
        L.append(f"> 💬 **{inline_posted}** inline comment(s) posted directly on code")
    if inline_failed > 0:
        L.append(f"> ⚠️ **{inline_failed}** comment(s) could not be posted inline — shown below")
    L.append("")

    # ── Critical Findings ──
    L.append("---")
    L.append("### 🔴 Critical Findings\n")
    if not critical:
        L.append("No critical issues found. ✅\n")
    for i, f in enumerate(critical, 1):
        L.append(f"#### {i}. {f.get('title', 'Issue')}")
        L.append(f"📍 **`{f.get('file', '?')}:{f.get('line', '?')}`**")
        dupes = f.get("duplicates", [])
        if dupes:
            L.append(f"↳ Also present in: {', '.join(f'`{d}`' for d in dupes)}")
        L.append("")
        L.append("**Vulnerable code:**")
        L.append(f"```java\n{f.get('code_snippet', '')}\n```")
        L.append(f"**Why this is critical:** {f.get('why_critical', '')}\n")
        L.append(f"**Example of misuse:** {f.get('exploit_example', '')}\n")
        fixes = f.get("fixes", [])
        if fixes:
            L.append("**Required Fix:**\n")
            for fix in fixes:
                L.append(f"<details><summary><b>{fix.get('option', 'Fix')}</b></summary>\n")
                L.append(f"```java\n{fix.get('code', '')}\n```")
                L.append(f"{fix.get('explanation', '')}\n")
                L.append("</details>\n")
        L.append("")

    # ── Security Findings ──
    L.append("---")
    L.append("### 🔒 Security Issues & Fixes\n")
    if not security:
        L.append("No security issues found. ✅\n")
    for i, f in enumerate(security, 1):
        L.append(f"#### {i}. {f.get('title', 'Issue')}")
        L.append(f"📍 **`{f.get('file', '?')}:{f.get('line', '?')}`**")
        dupes = f.get("duplicates", [])
        if dupes:
            L.append(f"↳ Also present in: {', '.join(f'`{d}`' for d in dupes)}")
        L.append("")
        L.append("**Vulnerable code:**")
        L.append(f"```java\n{f.get('code_snippet', '')}\n```")
        L.append(f"**Issue:** {f.get('description', '')}\n")
        L.append("**Fix:**")
        L.append(f"```java\n{f.get('fix_code', '')}\n```")
        L.append(f"_{f.get('fix_explanation', '')}_\n")

    # ── Recommendations ──
    if recs:
        L.append("---")
        L.append("### 💡 Recommendations\n")
        for i, r in enumerate(recs, 1):
            L.append(f"#### {i}. {r.get('title', 'Suggestion')}")
            L.append(f"📍 **`{r.get('file', '?')}:{r.get('line', '?')}`**")
            dupes = r.get("duplicates", [])
            if dupes:
                L.append(f"↳ Also in: {', '.join(f'`{d}`' for d in dupes)}")
            L.append("")
            L.append("**Current code:**")
            L.append(f"```java\n{r.get('code_snippet', '')}\n```")
            L.append(f"{r.get('description', '')}\n")
            L.append("**Suggested improvement:**")
            L.append(f"```java\n{r.get('suggested_code', '')}\n```\n")

    # ── Optimisations ──
    if optimisations:
        L.append("---")
        L.append("### ⚡ Optimisations\n")
        for opt in optimisations:
            L.append(f"- {opt}")
        L.append("")

    # ── Verdict ──
    L.append("---")
    L.append(f"### {verdict_emoji} Verdict: **{verdict.replace('_', ' ')}**\n")
    L.append(f"{verdict_reason}\n")

    # ── Next Steps ──
    if next_steps:
        L.append("### 📌 Next Steps\n")
        for j, step in enumerate(next_steps, 1):
            L.append(f"{j}. {step}")
        L.append("")

    L.append("---")
    L.append("*Powered by Claude via LiteLLM Proxy*")

    return "\n".join(L)


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

    # Task 1: Analyze with LLM
    print("Analyzing PR with Claude via LiteLLM...")
    findings_prompt = build_findings_prompt(
        pr_number, pr_title, pr_author, pr_description, file_list, diff
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": findings_prompt}
    ]

    try:
        raw_response = llm_client.chat(messages, max_tokens=4096, temperature=0.1)
    except Exception as e:
        progress.fail(f"LLM analysis failed: {e}")
        sys.exit(1)

    # Parse JSON
    try:
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]
        review_data = json.loads(cleaned)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Failed to parse LLM JSON: {e}")
        print(f"Raw (first 500): {raw_response[:500]}")
        fallback = f"## 🤖 Claude AI Review\n\n{raw_response}\n\n---\n*Powered by Claude via LiteLLM Proxy*"
        progress.finish(fallback)
        return

    all_findings = (
        review_data.get("critical_findings", []) +
        review_data.get("security_findings", []) +
        review_data.get("recommendations", [])
    )
    print(f"LLM returned {len(all_findings)} finding(s), verdict: {review_data.get('verdict')}")
    progress.complete_task(1, next_task=2)

    # Task 2: Post inline comments
    inline_posted = 0
    inline_failed = 0
    review_comments = []

    if head_sha and all_findings:
        for f in all_findings:
            file_path = f.get("file", "")
            line = f.get("line", 0)
            position = diff_line_map.get(file_path, {}).get(line)
            if not position:
                inline_failed += 1
                continue

            # Determine severity icon
            if f in review_data.get("critical_findings", []):
                sev_icon, sev_label = "🔴", "Critical"
            elif f in review_data.get("security_findings", []):
                sev_icon, sev_label = "🔒", "Security"
            else:
                sev_icon, sev_label = "💡", "Recommendation"

            body_parts = [f"{sev_icon} **{sev_label}: {f.get('title', 'Issue')}**\n"]

            desc = f.get("why_critical") or f.get("description") or ""
            if desc:
                body_parts.append(f"{desc}\n")

            # Add fix code if available
            fix_code = None
            if "fixes" in f and f["fixes"]:
                fix_code = f["fixes"][0].get("code", "")
                fix_expl = f["fixes"][0].get("explanation", "")
            elif "fix_code" in f:
                fix_code = f.get("fix_code", "")
                fix_expl = f.get("fix_explanation", "")
            elif "suggested_code" in f:
                fix_code = f.get("suggested_code", "")
                fix_expl = ""

            if fix_code:
                body_parts.append(f"💡 **Suggested fix:**\n```java\n{fix_code}\n```")
                if fix_expl:
                    body_parts.append(f"_{fix_expl}_")

            review_comments.append({
                "path": file_path,
                "position": position,
                "body": "\n".join(body_parts)
            })

        if review_comments:
            try:
                verdict = review_data.get("verdict", "COMMENT")
                event = {"APPROVE": "APPROVE",
                         "REQUEST_CHANGES": "REQUEST_CHANGES"}.get(verdict, "COMMENT")
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

    # Task 3: Compile and post summary
    print("Compiling review summary...")
    summary_md = build_summary_markdown(
        review_data, pr_number, pr_title, pr_author,
        inline_posted, inline_failed
    )
    progress.finish(summary_md)
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
