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
        """Get PR diff using gh CLI"""
        env = {**os.environ, "GH_TOKEN": self.token}
        result = subprocess.run(
            ["gh", "pr", "diff", str(pr_number)],
            capture_output=True, text=True, check=True, env=env
        )
        return result.stdout

    def get_pr_files(self, pr_number: int) -> List[Dict[str, Any]]:
        """Get list of changed files with patch info"""
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

    def get_pr_head_sha(self, pr_number: int) -> str:
        """Get the HEAD commit SHA of a PR"""
        url = f"{self.base_url}/repos/{self.repo}/pulls/{pr_number}"
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()["head"]["sha"]

    def post_pr_comment(self, pr_number: int, body: str) -> Dict[str, Any]:
        """Post an issue-level comment on the PR. Returns the comment object."""
        url = f"{self.base_url}/repos/{self.repo}/issues/{pr_number}/comments"
        response = requests.post(
            url, headers=self.headers, json={"body": body}, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def update_pr_comment(self, comment_id: int, body: str) -> Dict[str, Any]:
        """Edit an existing issue comment by its ID."""
        url = f"{self.base_url}/repos/{self.repo}/issues/comments/{comment_id}"
        response = requests.patch(
            url, headers=self.headers, json={"body": body}, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def submit_review(self, pr_number: int, commit_id: str, body: str,
                      comments: List[Dict[str, Any]], event: str = "COMMENT"):
        """Submit a pull request review with inline comments."""
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
# Live progress tracker — edits a single PR comment as tasks complete
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Posts and updates a single PR comment to show review progress.

    Creates an initial comment with a task checklist, then edits it
    as each step completes. When the review is finished the comment
    is replaced with the full review results.
    """

    TASKS = [
        "Fetch PR diff and changed files",
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
        """Build the markdown body showing task progress."""
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
        lines.append("")
        lines.append(f"*Running for {elapsed}s...*")
        lines.append("")
        lines.append("---")
        lines.append("*Powered by Claude via LiteLLM Proxy*")
        return "\n".join(lines)

    def start(self):
        """Post the initial progress comment on the PR."""
        body = self._build_progress_body(active_task=0)
        try:
            result = self.github_api.post_pr_comment(self.pr_number, body)
            self.comment_id = result["id"]
            print(f"Progress comment posted (id={self.comment_id})")
        except Exception as e:
            print(f"Warning: could not post progress comment: {e}")

    def complete_task(self, task_index: int, next_task: Optional[int] = None):
        """Mark a task as done and optionally start the next one."""
        self.completed.append(task_index)
        if self.comment_id is None:
            return
        body = self._build_progress_body(active_task=next_task)
        try:
            self.github_api.update_pr_comment(self.comment_id, body)
        except Exception as e:
            print(f"Warning: could not update progress comment: {e}")

    def finish(self, final_body: str):
        """Replace the progress comment with the final review body."""
        elapsed = int(time.time() - self.start_time)
        header = (
            f"**Claude AI finished reviewing in {elapsed}s**\n\n---\n\n"
        )
        full_body = header + final_body
        if self.comment_id is None:
            # Progress comment was never created — post fresh
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
                print(f"Warning: could not update progress comment, posting new: {e}")
                try:
                    self.github_api.post_pr_comment(self.pr_number, full_body)
                except Exception as e2:
                    print(f"Failed to post final comment: {e2}")
                    sys.exit(1)

    def fail(self, error_message: str):
        """Update the progress comment to show failure."""
        elapsed = int(time.time() - self.start_time)
        lines = [
            f"## 🤖 Claude AI Review — PR #{self.pr_number}",
            "",
            f"❌ **Review failed after {elapsed}s**",
            "",
            "### Tasks",
        ]
        for i, task in enumerate(self.TASKS):
            if i in self.completed:
                lines.append(f"- [x] {task}")
            else:
                lines.append(f"- [ ] {task}")
        lines.append("")
        lines.append(f"**Error:** {error_message}")
        lines.append("")
        lines.append("---")
        lines.append("*Powered by Claude via LiteLLM Proxy*")
        body = "\n".join(lines)

        if self.comment_id:
            try:
                self.github_api.update_pr_comment(self.comment_id, body)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Diff-line mapping: translate file + new-file line → diff position
# ---------------------------------------------------------------------------

def build_diff_line_map(files: List[Dict[str, Any]]) -> Dict[str, Dict[int, int]]:
    """Parse patch hunks to map (file, new_line) → diff position.

    GitHub inline review comments require a *position* within the diff,
    not the absolute line number.  This function parses every file's
    ``patch`` text and builds the mapping.
    """
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

SYSTEM_PROMPT = """You are a senior code reviewer for Apache Atlas Metastore — a Java 17 / Maven
project that implements a metadata catalog built on JanusGraph, Cassandra,
Elasticsearch, and Kafka.

## Project Layout
- `repository/`  — core business logic, entity preprocessors
- `webapp/`      — REST API layer
- `intg/`        — API models & client libraries
- `helm/`        — Helm charts
- `.github/`     — CI/CD workflows & scripts

## What to Look For
1. **Bugs & Runtime Issues** — NPEs, resource leaks, race conditions, unclosed transactions
2. **Security** — injection, missing auth checks, leaked secrets, input validation gaps
3. **Performance** — N+1 queries, unnecessary allocations in hot paths, missing caching
4. **Graph DB** — JanusGraph tx handling, index usage, vertex/edge access patterns
5. **Backward Compatibility** — REST contract changes, TypeDef changes, Kafka format changes

## Rules
- ONLY flag real problems or concrete improvements — never comment on style/formatting.
- If the PR is clean, return an empty findings list.
- Fewer high-quality findings >> many low-value ones."""


def build_findings_prompt(pr_number: int, pr_title: str, pr_author: str,
                          file_list: List[str], diff: str) -> str:
    """Prompt that asks the LLM to return structured JSON findings."""
    return f"""Review this PR and return your findings as **pure JSON** (no markdown fences, no extra text).

## PR
- **#{pr_number}** — {pr_title}
- **Author:** {pr_author}
- **Changed files:** {', '.join(file_list) if file_list else 'N/A'}

## Diff
```diff
{diff}
```

## Response Schema

Return a JSON object with exactly these keys:

{{
  "summary": "<2-3 sentence overview of what the PR does and overall quality>",
  "verdict": "APPROVE | APPROVE_WITH_COMMENTS | REQUEST_CHANGES",
  "verdict_reason": "<one sentence justification>",
  "findings": [
    {{
      "file": "<relative file path, e.g. src/Foo.java>",
      "line": <integer — the line number in the NEW file where the issue is>,
      "severity": "critical | warning | suggestion",
      "category": "bug | security | performance | compatibility | improvement",
      "title": "<short one-line title>",
      "description": "<2-3 sentence explanation of WHY this is a problem and its IMPACT>",
      "suggestion": "<concrete fix — either a short code snippet or a one-line description>"
    }}
  ]
}}

### IMPORTANT RULES for findings:
- `file` must be an EXACT path from the changed files list above.
- `line` must be a line number that exists in the NEW version of the file (right side of the diff, lines starting with `+` or unchanged context lines).
- Do NOT reference deleted lines (lines starting with `-`).
- Only include findings for real issues. If the PR is clean, return an empty `findings` array.
- Maximum 10 findings. Prioritize by severity.
- Each finding MUST explain WHY it's a problem, not just WHAT the code does.
"""


def build_summary_markdown(summary: str, verdict: str, verdict_reason: str,
                           findings: List[Dict], pr_number: int,
                           pr_title: str, pr_author: str,
                           inline_posted: int, inline_failed: int) -> str:
    """Build the rich summary comment from structured findings."""

    verdict_emoji = {
        "APPROVE": "✅",
        "APPROVE_WITH_COMMENTS": "⚠️",
        "REQUEST_CHANGES": "❌"
    }.get(verdict, "ℹ️")
    verdict_display = verdict.replace("_", " ")

    # Group findings by severity
    critical = [f for f in findings if f.get("severity") == "critical"]
    warnings = [f for f in findings if f.get("severity") == "warning"]
    suggestions = [f for f in findings if f.get("severity") == "suggestion"]

    # Group findings by category
    security = [f for f in findings if f.get("category") == "security"]
    perf = [f for f in findings if f.get("category") == "performance"]

    lines = []
    lines.append(f"## 🤖 Claude AI Review — PR #{pr_number}\n")

    # --- Tasks completed ---
    lines.append("### Tasks")
    lines.append("- [x] Fetch PR diff and changed files")
    lines.append("- [x] Analyze code with Claude AI")
    lines.append("- [x] Post inline review comments")
    lines.append("- [x] Compile review summary")
    lines.append("")

    # --- Summary ---
    lines.append("### 📋 Summary\n")
    lines.append(f"{summary}\n")

    # --- Stats bar ---
    total = len(findings)
    lines.append(f"> **{total} finding(s):** "
                 f"🔴 {len(critical)} critical · "
                 f"🟡 {len(warnings)} warning · "
                 f"🔵 {len(suggestions)} suggestion")
    if inline_posted > 0:
        lines.append(f"> 💬 {inline_posted} inline comment(s) posted on code")
    if inline_failed > 0:
        lines.append(f"> ⚠️ {inline_failed} comment(s) could not be posted inline (shown below)")
    lines.append("")

    # --- Critical Findings ---
    lines.append("### 🔴 Critical Findings\n")
    if critical:
        for i, f in enumerate(critical, 1):
            lines.append(f"**{i}. `{f['file']}:{f['line']}` — {f['title']}**")
            lines.append(f"> {f['description']}")
            if f.get("suggestion"):
                lines.append(f"> 💡 **Fix:** {f['suggestion']}")
            lines.append("")
    else:
        lines.append("None identified. ✅\n")

    # --- Security Findings ---
    if security:
        lines.append("### 🔒 Security Findings\n")
        for i, f in enumerate(security, 1):
            sev_icon = {"critical": "🔴", "warning": "🟡", "suggestion": "🔵"}.get(f["severity"], "")
            lines.append(f"**{i}. {sev_icon} `{f['file']}:{f['line']}` — {f['title']}**")
            lines.append(f"> {f['description']}")
            if f.get("suggestion"):
                lines.append(f"> 💡 **Fix:** {f['suggestion']}")
            lines.append("")

    # --- Warnings ---
    non_security_warnings = [f for f in warnings if f.get("category") != "security"]
    if non_security_warnings:
        lines.append("### 🟡 Warnings\n")
        for i, f in enumerate(non_security_warnings, 1):
            lines.append(f"**{i}. `{f['file']}:{f['line']}` — {f['title']}**")
            lines.append(f"> {f['description']}")
            if f.get("suggestion"):
                lines.append(f"> 💡 **Fix:** {f['suggestion']}")
            lines.append("")

    # --- Suggestions / Improvements ---
    if suggestions:
        lines.append("### 🔵 Suggestions & Improvements\n")
        for i, f in enumerate(suggestions, 1):
            lines.append(f"**{i}. `{f['file']}:{f['line']}` — {f['title']}**")
            lines.append(f"> {f['description']}")
            if f.get("suggestion"):
                lines.append(f"> 💡 **Fix:** {f['suggestion']}")
            lines.append("")

    # --- Performance ---
    if perf:
        lines.append("### ⚡ Performance Observations\n")
        for i, f in enumerate(perf, 1):
            sev_icon = {"critical": "🔴", "warning": "🟡", "suggestion": "🔵"}.get(f["severity"], "")
            lines.append(f"**{i}. {sev_icon} `{f['file']}:{f['line']}` — {f['title']}**")
            lines.append(f"> {f['description']}")
            if f.get("suggestion"):
                lines.append(f"> 💡 **Fix:** {f['suggestion']}")
            lines.append("")

    # --- Conclusion ---
    lines.append(f"### {verdict_emoji} Verdict: **{verdict_display}**\n")
    lines.append(f"{verdict_reason}\n")

    lines.append("---")
    lines.append("*Powered by Claude via LiteLLM Proxy*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main review logic
# ---------------------------------------------------------------------------

def review_pr(llm_client: LiteLLMClient, github_api: GitHubAPI,
              pr_number: int, pr_title: str, pr_author: str):
    """Fetch PR data, call LLM for structured findings, post inline + summary."""

    print(f"Reviewing PR #{pr_number}: {pr_title}")
    print(f"Author: {pr_author}")

    # --- Post initial progress comment ---
    progress = ProgressTracker(github_api, pr_number, pr_title)
    progress.start()

    # Task 0: Fetch PR diff and changed files
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

    print("Fetching PR head SHA...")
    try:
        head_sha = github_api.get_pr_head_sha(pr_number)
    except Exception as e:
        print(f"Could not fetch head SHA: {e}")
        head_sha = None

    diff_line_map = build_diff_line_map(files)
    progress.complete_task(0, next_task=1)

    # Task 1: Analyze code with Claude AI
    print("Analyzing PR with Claude via LiteLLM...")
    findings_prompt = build_findings_prompt(
        pr_number, pr_title, pr_author, file_list, diff
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": findings_prompt}
    ]

    try:
        raw_response = llm_client.chat(messages, max_tokens=4000, temperature=0.1)
    except Exception as e:
        progress.fail(f"LLM analysis failed: {e}")
        sys.exit(1)

    # Parse JSON response
    try:
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]
        review_data = json.loads(cleaned)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Failed to parse LLM JSON response: {e}")
        print(f"Raw response (first 500 chars): {raw_response[:500]}")
        # Fallback: post raw response
        fallback = (
            f"## 🤖 Claude AI Review\n\n{raw_response}\n\n---\n"
            f"*Powered by Claude via LiteLLM Proxy*"
        )
        progress.finish(fallback)
        return

    summary = review_data.get("summary", "No summary provided.")
    verdict = review_data.get("verdict", "APPROVE_WITH_COMMENTS")
    verdict_reason = review_data.get("verdict_reason", "")
    findings = review_data.get("findings", [])

    print(f"LLM returned {len(findings)} finding(s), verdict: {verdict}")
    progress.complete_task(1, next_task=2)

    # Task 2: Post inline review comments
    inline_posted = 0
    inline_failed = 0
    review_comments = []

    if head_sha and findings:
        for f in findings:
            file_path = f.get("file", "")
            line = f.get("line", 0)

            file_map = diff_line_map.get(file_path, {})
            position = file_map.get(line)

            if not position:
                inline_failed += 1
                continue

            sev_icon = {"critical": "🔴", "warning": "🟡", "suggestion": "🔵"}.get(
                f.get("severity", ""), "ℹ️")
            cat_label = f.get("category", "").capitalize()

            comment_body = (
                f"{sev_icon} **{cat_label}: {f.get('title', 'Issue')}**\n\n"
                f"{f.get('description', '')}\n"
            )
            if f.get("suggestion"):
                comment_body += f"\n💡 **Suggested fix:** {f['suggestion']}\n"

            review_comments.append({
                "path": file_path,
                "position": position,
                "body": comment_body
            })

        if review_comments:
            try:
                event = {
                    "APPROVE": "APPROVE",
                    "REQUEST_CHANGES": "REQUEST_CHANGES"
                }.get(verdict, "COMMENT")

                github_api.submit_review(
                    pr_number, head_sha,
                    body="",
                    comments=review_comments,
                    event=event
                )
                inline_posted = len(review_comments)
                print(f"Posted {inline_posted} inline comment(s) via review API.")
            except Exception as e:
                print(f"Failed to submit review with inline comments: {e}")
                inline_failed += len(review_comments)
                inline_posted = 0

    progress.complete_task(2, next_task=3)

    # Task 3: Compile and post review summary
    print("Compiling review summary...")
    summary_md = build_summary_markdown(
        summary, verdict, verdict_reason, findings,
        pr_number, pr_title, pr_author,
        inline_posted, inline_failed
    )

    progress.finish(summary_md)
    print("Review posted successfully!")


def main():
    parser = argparse.ArgumentParser(description="Claude PR Review via LiteLLM")
    parser.add_argument("--pr-number", type=int, required=True, help="PR number")
    parser.add_argument("--pr-title", required=True, help="PR title")
    parser.add_argument("--pr-author", required=True, help="PR author")
    parser.add_argument("--repo", required=True, help="GitHub repository (owner/repo)")
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
