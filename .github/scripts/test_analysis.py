#!/usr/bin/env python3
"""
Test Analysis Script using LiteLLM Proxy
Analyzes test results after integration tests complete
"""

import os
import sys
import json
import glob
import argparse
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Tuple

from litellm_client import LiteLLMClient


class TestResultParser:
    """Parse JUnit XML test results"""

    @staticmethod
    def parse_junit_xml(xml_path: str) -> Tuple[int, int, int, int, List[str]]:
        """Parse JUnit XML and extract test counts and failures"""
        tree = ET.parse(xml_path)
        root = tree.getroot()

        total = int(root.attrib.get('tests', 0))
        failures = int(root.attrib.get('failures', 0))
        errors = int(root.attrib.get('errors', 0))
        skipped = int(root.attrib.get('skipped', 0))

        failed_tests = []
        for testcase in root.findall('.//testcase'):
            name = testcase.attrib.get('name', 'Unknown')
            classname = testcase.attrib.get('classname', '')

            failure = testcase.find('failure')
            error = testcase.find('error')

            if failure is not None or error is not None:
                message = failure.attrib.get('message', '') if failure is not None else error.attrib.get('message', '')
                failed_tests.append(f"{classname}.{name}: {message}")

        return total, failures + errors, skipped, total - failures - errors - skipped, failed_tests

    @staticmethod
    def parse_junit_directory(directory: str) -> Tuple[int, int, int, int, List[str]]:
        """Parse all JUnit XML files in a directory and aggregate results."""
        total = 0
        failed = 0
        skipped = 0
        passed = 0
        all_failed_tests = []

        xml_files = glob.glob(os.path.join(directory, "TEST-*.xml"))
        for xml_path in xml_files:
            try:
                t, f, s, p, ft = TestResultParser.parse_junit_xml(xml_path)
                total += t
                failed += f
                skipped += s
                passed += p
                all_failed_tests.extend(ft)
            except Exception as e:
                print(f"Warning: Failed to parse {xml_path}: {e}")

        return total, failed, skipped, passed, all_failed_tests


def analyze_tests(
    llm_client: LiteLLMClient,
    pr_number: int,
    pr_title: str,
    test_status: str,
    total: int,
    passed: int,
    failed: int,
    skipped: int,
    failed_tests: List[str],
    pr_diff: str
):
    """Analyze test results and provide insights"""

    print(f"Test Results: {passed} passed, {failed} failed, {skipped} skipped (total: {total})")

    if failed == 0:
        focus = "Analyze test coverage for changed code and identify any gaps."
    else:
        focus = "Analyze why tests failed and provide concrete fix suggestions."

    prompt = f"""You are analyzing integration test results for a PR in Apache Atlas Metastore.
Keep your entire response under 60 lines. Be concise and scannable.

## Context
- **PR:** #{pr_number} — {pr_title}
- **Test Status:** {test_status}
- **Results:** {passed} passed, {failed} failed, {skipped} skipped (total: {total})

## Failed Tests
{chr(10).join(failed_tests[:20]) if failed_tests else "None"}

## PR Changes (Diff)
```diff
{pr_diff[:10000]}
```

## Task
{focus}

## Output Format — STRICT

### Results
| Status | Count |
|--------|-------|
| ✅ Passed | {passed} |
| ❌ Failed | {failed} |
| ⏭️ Skipped | {skipped} |

### {'Root Cause Analysis' if failed > 0 else 'Coverage Assessment'}
[Numbered list — each item: one failed test → probable cause → affected file:line from the diff]
[If no failures: brief coverage assessment in 3-5 bullet points]

### {'Recommended Fixes' if failed > 0 else 'Recommendations'}
[Numbered list — each item: one-line fix description with short code snippet (max 3 lines) if applicable]
[If no failures: coverage gap recommendations in 2-3 bullet points]

### Verdict
[One of: ✅ Tests look good | ⚠️ Minor gaps | ❌ Failures need attention]
[One sentence summary of next steps]
"""

    messages = [{"role": "user", "content": prompt}]

    print("Analyzing test results with Claude...")
    try:
        analysis = llm_client.chat(messages, max_tokens=3000, temperature=0.1)
        return analysis
    except Exception as e:
        print(f"Failed to analyze tests: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Test Analysis via LiteLLM")
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--pr-title", required=True)
    parser.add_argument("--test-status", required=True)
    parser.add_argument("--junit-xml", default=None, help="Path to a single JUnit XML file")
    parser.add_argument("--junit-dir", default=None, help="Path to directory containing JUnit XML files")
    parser.add_argument("--pr-diff-file", default=None, help="Path to file containing PR diff")
    parser.add_argument("--litellm-url", default="https://llmproxy.atlan.dev/v1")
    parser.add_argument("--litellm-key", default=None, help="LiteLLM API key (or set LITELLM_API_KEY env var)")
    parser.add_argument("--model", default="claude")
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")

    args = parser.parse_args()

    # Resolve secrets from args or environment
    litellm_key = args.litellm_key or os.environ.get("LITELLM_API_KEY")
    if not litellm_key:
        print("Error: LiteLLM API key required. Use --litellm-key or set LITELLM_API_KEY env var.")
        sys.exit(1)

    # Parse test results
    if args.junit_dir:
        print(f"Parsing test results from directory {args.junit_dir}...")
        total, failed, skipped, passed, failed_tests = TestResultParser.parse_junit_directory(args.junit_dir)
    elif args.junit_xml:
        print(f"Parsing test results from {args.junit_xml}...")
        try:
            total, failed, skipped, passed, failed_tests = TestResultParser.parse_junit_xml(args.junit_xml)
        except Exception as e:
            print(f"Failed to parse test results: {e}")
            sys.exit(1)
    else:
        print("Error: Provide --junit-xml or --junit-dir")
        sys.exit(1)

    # Read PR diff from file
    pr_diff = ""
    if args.pr_diff_file:
        try:
            with open(args.pr_diff_file, 'r') as f:
                pr_diff = f.read()
        except Exception as e:
            print(f"Warning: Could not read PR diff file: {e}")

    # Initialize LLM client
    llm_client = LiteLLMClient(args.litellm_url, litellm_key, args.model)

    # Analyze
    analysis = analyze_tests(
        llm_client,
        args.pr_number,
        args.pr_title,
        args.test_status,
        total,
        passed,
        failed,
        skipped,
        failed_tests,
        pr_diff
    )

    if analysis:
        output_content = f"""## Integration Test Analysis

{analysis}

---
*Powered by Claude via LiteLLM Proxy*
"""

        if args.output == "-":
            print(output_content)
        else:
            with open(args.output, 'w') as f:
                f.write(output_content)
            print(f"Analysis written to {args.output}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
